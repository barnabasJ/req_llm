defmodule ReqLLM.TimeoutBudget do
  @moduledoc false

  alias ReqLLM.Error.API.Timeout

  @capture_key :req_llm_timeout_capture

  @type budget_timeout :: pos_integer() | :infinity
  @type deadline ::
          :infinity
          | %{
              required(:timeout) => pos_integer(),
              required(:expires_at) => integer(),
              optional(:clock) => {module(), term()}
            }

  @spec total_timeout(keyword()) :: budget_timeout()
  def total_timeout(opts) do
    opts
    |> Keyword.get(:total_timeout, Application.get_env(:req_llm, :total_timeout, :infinity))
    |> validate_timeout!(:total_timeout)
  end

  @spec stream_idle_timeout(keyword()) :: {:configured, budget_timeout()} | :legacy
  def stream_idle_timeout(opts) do
    case Keyword.fetch(opts, :stream_idle_timeout) do
      {:ok, timeout} -> {:configured, validate_timeout!(timeout, :stream_idle_timeout)}
      :error -> configured_stream_idle_timeout()
    end
  end

  @spec deadline(keyword()) :: deadline()
  def deadline(opts) do
    timeout = total_timeout(opts)
    monotonic_now = now()
    schema = ReqLLM.Provider.Options.generation_subset_schema([:total_deadline_at])
    validated = NimbleOptions.validate!(Keyword.take(opts, [:total_deadline_at]), schema)

    case Keyword.fetch(validated, :total_deadline_at) do
      {:ok, absolute} ->
        remaining = max(absolute - System.system_time(:millisecond), 0)
        budget = if timeout == :infinity, do: remaining, else: min(timeout, remaining)
        %{timeout: max(budget, 1), expires_at: monotonic_now + budget}

      :error when timeout == :infinity ->
        :infinity

      :error ->
        %{timeout: timeout, expires_at: monotonic_now + timeout}
    end
  end

  @spec remaining(deadline()) :: budget_timeout() | 0
  def remaining(:infinity), do: :infinity

  def remaining(%{expires_at: expires_at} = deadline) do
    max(expires_at - clock_now(deadline), 0)
  end

  @doc false
  def clamp_options(options, :infinity), do: {:ok, options, []}

  def clamp_options(options, deadline) do
    case remaining(deadline) do
      0 ->
        {:error, error(:total, deadline.timeout)}

      budget ->
        keys = [:pool_timeout, :receive_timeout, :request_timeout]

        total_bound_waits =
          Enum.filter(keys, &bounded_by_total?(Keyword.get(options, &1, :infinity), budget))

        bounded =
          Enum.reduce(keys, options, fn key, opts ->
            Keyword.update(opts, key, budget, &clamp_timeout(&1, budget))
          end)

        {:ok, bounded, total_bound_waits}
    end
  end

  @doc false
  def total_timeout_for_wait(:infinity, _total_bound_waits, _waits), do: nil

  def total_timeout_for_wait(deadline, total_bound_waits, waits) do
    if Enum.all?(waits, &(&1 in total_bound_waits)) and remaining(deadline) == 0,
      do: error(:total, deadline.timeout)
  end

  @doc false
  def normalize_transport_timeout(
        %{__struct__: module, reason: :timeout} = reason,
        deadline,
        total_bound_waits
      )
      when module in [Req.TransportError, Mint.TransportError, Finch.TransportError, Finch.Error] do
    total_timeout_for_wait(deadline, total_bound_waits, [:receive_timeout, :request_timeout]) ||
      reason
  end

  def normalize_transport_timeout(
        %Finch.Error{reason: :request_timeout} = reason,
        deadline,
        total_bound_waits
      ) do
    total_timeout_for_wait(deadline, total_bound_waits, [:request_timeout]) || reason
  end

  def normalize_transport_timeout(reason, _deadline, _total_bound_waits), do: reason

  defp bounded_by_total?(:infinity, _budget), do: true
  defp bounded_by_total?(timeout, budget) when is_integer(timeout), do: timeout >= budget
  defp bounded_by_total?(_timeout, _budget), do: false

  @doc false
  def retry_delay(:infinity, delay), do: {:ok, delay}

  def retry_delay(deadline, delay) do
    case remaining(deadline) do
      0 -> {:error, error(:total, deadline.timeout)}
      budget -> {:ok, min(delay, budget)}
    end
  end

  @doc false
  def wait(deadline, delay) do
    with {:ok, bounded} <- retry_delay(deadline, delay) do
      clock_sleep(deadline, bounded)

      case retry_delay(deadline, 0) do
        {:ok, _} -> :ok
        error -> error
      end
    end
  end

  @doc false
  def without_transport_deadline(opts), do: Keyword.delete(opts, :total_timeout_deadline)

  @doc false
  def request_deadline(request),
    do: Req.Request.get_private(request, :req_llm_timeout_deadline, :infinity)

  defp clamp_timeout(:infinity, budget), do: budget
  defp clamp_timeout(timeout, budget) when is_integer(timeout), do: min(timeout, budget)
  defp clamp_timeout(timeout, _budget), do: timeout

  # The optional capability belongs to the transient internal deadline, not model
  # configuration. Tests supply a clock without sleeping or changing global time.
  defp clock_now(%{clock: {module, context}}), do: module.now(context)
  defp clock_now(_deadline), do: now()
  defp clock_sleep(%{clock: {module, context}}, delay), do: module.sleep(context, delay)
  defp clock_sleep(_deadline, 0), do: :ok
  defp clock_sleep(_deadline, delay), do: Process.sleep(delay)

  @spec error(:total | :stream_idle, pos_integer()) :: Timeout.t()
  def error(kind, timeout) do
    Timeout.exception(kind: kind, timeout: timeout)
  end

  @spec request(Req.Request.t(), deadline()) ::
          {:ok, Req.Response.t()} | {:error, Exception.t()}
  @spec request(Req.Request.t(), deadline(), keyword()) ::
          {:ok, Req.Response.t()} | {:error, Exception.t()}
  def request(request, deadline, opts) do
    schema = ReqLLM.Provider.Options.generation_subset_schema([:on_response_status])
    validated = NimbleOptions.validate!(Keyword.take(opts, [:on_response_status]), schema)

    request
    |> Req.Request.put_private(:req_llm_response_status_observer, validated[:on_response_status])
    |> request(deadline)
  end

  def request(%Req.Request{} = request, :infinity) do
    request |> ReqLLM.Transport.attach() |> Req.request()
  end

  def request(%Req.Request{} = request, %{timeout: timeout} = deadline) do
    capture_ref = make_ref()

    request =
      request
      |> ReqLLM.Transport.attach()
      |> Req.Request.put_private(:req_llm_timeout_deadline, deadline)
      |> Req.Request.put_private(@capture_key, %{owner: self(), ref: capture_ref})
      |> Req.Request.append_request_steps(
        llm_timeout_capture: &__MODULE__.capture_request_context/1
      )

    case remaining(deadline) do
      0 ->
        timeout_result(request, capture_ref, timeout)

      _remaining ->
        request
        |> run_request_task(deadline)
        |> resolve_request_task(request, capture_ref, timeout)
    end
  end

  @doc false
  @spec capture_request_context(Req.Request.t()) :: Req.Request.t()
  def capture_request_context(%Req.Request{} = request) do
    case request.private[@capture_key] do
      %{owner: owner, ref: ref} ->
        send(owner, {__MODULE__, ref, ReqLLM.Telemetry.request_context(request)})

      _other ->
        :ok
    end

    request
  end

  defp configured_stream_idle_timeout do
    case Application.fetch_env(:req_llm, :stream_idle_timeout) do
      {:ok, timeout} -> {:configured, validate_timeout!(timeout, :stream_idle_timeout)}
      :error -> :legacy
    end
  end

  defp validate_timeout!(:infinity, _key), do: :infinity
  defp validate_timeout!(timeout, _key) when is_integer(timeout) and timeout > 0, do: timeout

  defp validate_timeout!(timeout, key) do
    raise ReqLLM.Error.Invalid.Parameter.exception(
            parameter: "#{key} must be a positive integer or :infinity, got: #{inspect(timeout)}"
          )
  end

  defp run_request_task(request, deadline) do
    owner = self()

    task =
      Task.Supervisor.async_nolink(ReqLLM.TaskSupervisor, fn ->
        request_pid = self()

        {:ok, _guardian} =
          Task.Supervisor.start_child(ReqLLM.TaskSupervisor, fn ->
            watch_request_owner(owner, request_pid)
          end)

        Req.request(request)
      end)

    {task, Task.yield(task, remaining(deadline))}
  end

  defp watch_request_owner(owner, request_pid) do
    owner_ref = Process.monitor(owner)
    request_ref = Process.monitor(request_pid)

    receive do
      {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
        Process.exit(request_pid, :kill)

      {:DOWN, ^request_ref, :process, ^request_pid, _reason} ->
        :ok
    end
  end

  defp resolve_request_task({_task, {:ok, result}}, _request, capture_ref, _timeout) do
    discard_captured_context(capture_ref)
    result
  end

  defp resolve_request_task({_task, {:exit, reason}}, _request, capture_ref, _timeout) do
    discard_captured_context(capture_ref)
    propagate_task_exit(reason)
  end

  defp resolve_request_task({task, nil}, request, capture_ref, timeout) do
    case Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        discard_captured_context(capture_ref)
        result

      {:exit, reason} ->
        discard_captured_context(capture_ref)
        propagate_task_exit(reason)

      nil ->
        timeout_result(request, capture_ref, timeout)
    end
  end

  defp timeout_result(request, capture_ref, timeout) do
    error = error(:total, timeout)
    emit_timeout_telemetry(request, capture_ref, error)
    {:error, error}
  end

  defp emit_timeout_telemetry(request, capture_ref, error) do
    context = captured_context(capture_ref) || ReqLLM.Telemetry.request_context(request)

    if context do
      ReqLLM.Telemetry.exception_request(context, error)
    end

    :ok
  end

  defp captured_context(capture_ref, latest \\ nil) do
    receive do
      {__MODULE__, ^capture_ref, context} -> captured_context(capture_ref, context)
    after
      0 -> latest
    end
  end

  defp discard_captured_context(capture_ref) do
    captured_context(capture_ref)
    :ok
  end

  defp propagate_task_exit({%{__exception__: true} = exception, stacktrace})
       when is_list(stacktrace) do
    reraise exception, stacktrace
  end

  defp propagate_task_exit(reason), do: exit(reason)

  defp now, do: System.monotonic_time(:millisecond)
end
