defmodule ReqLLM.RequestBudgetTest do
  use ExUnit.Case, async: false
  @moduletag category: :core
  alias ReqLLM.TestSupport.RequestBudgetClock, as: Clock

  defmodule BoundaryProvider do
    def stream_transport(_model, opts) do
      send(opts[:test_pid], {:callback_options, :transport, opts})
      opts[:test_transport]
    end

    def stream_protocol_parser(_model, opts) do
      send(opts[:test_pid], {:callback_options, :parser, opts})
      nil
    end

    def attach_stream(_model, _context, opts, _finch) do
      send(opts[:test_pid], {:callback_options, :http, opts})
      {:error, :boundary_checked}
    end

    def attach_websocket_stream(_model, _context, opts) do
      send(opts[:test_pid], {:callback_options, :websocket, opts})
      {:error, :boundary_checked}
    end

    def attach_in_process_stream(_model, _context, opts) do
      send(opts[:test_pid], {:callback_options, :in_process, opts})
      {:error, :boundary_checked}
    end
  end

  setup do
    {:ok, clock} = Clock.start()
    %{clock: clock, deadline: Clock.deadline(clock)}
  end

  test "stream retries clamp each wire wait to one deadline and refuse any next attempt after expiry",
       ctx do
    wire = fn _request, _finch, acc, _callback, options ->
      Clock.call(ctx.clock, options)
      Clock.advance(ctx.clock, 30)
      {:error, %Req.TransportError{reason: :timeout}, acc}
    end

    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total, timeout: 100}, []} =
             stream(ctx, wire, max_retries: 10)

    assert Enum.map(Clock.state(ctx.clock).calls, fn {time, options} ->
             assert options[:receive_timeout] == options[:pool_timeout]
             assert options[:receive_timeout] == options[:request_timeout]
             {time, options[:receive_timeout]}
           end) == [{0, 100}, {30, 70}, {60, 40}, {90, 10}]

    assert Clock.state(ctx.clock).waits == [0, 0, 0]
  end

  test "HTTP retry-after backoff is clamped and no second wire attempt starts at expiry", ctx do
    wire = fn _request, _finch, acc, callback, options ->
      Clock.call(ctx.clock, options)
      acc = callback.({:status, 429}, acc)
      acc = callback.({:headers, [{"retry-after", "1"}]}, acc)
      {:ok, acc}
    end

    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total}, []} =
             stream(ctx, wire, max_retries: 5)

    assert length(Clock.state(ctx.clock).calls) == 1
    assert Clock.state(ctx.clock).waits == [100]
    assert Clock.now(ctx.clock) == 100
  end

  test "expiry before inner backoff refuses the delay and the next wire attempt", ctx do
    wire = fn _request, _finch, acc, _callback, options ->
      Clock.call(ctx.clock, options)
      Clock.advance(ctx.clock, 100)
      {:error, %Req.TransportError{reason: :timeout}, acc}
    end

    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total}, []} =
             stream(ctx, wire, max_retries: 5)

    assert length(Clock.state(ctx.clock).calls) == 1
    assert Clock.state(ctx.clock).waits == []
  end

  test "partial data still disables transport replay", ctx do
    wire = fn _request, _finch, acc, callback, options ->
      Clock.call(ctx.clock, options)
      acc = callback.({:status, 200}, acc)
      acc = callback.({:data, "partial"}, acc)
      {:error, %Req.TransportError{reason: :timeout}, acc}
    end

    assert {:error, %Req.TransportError{}, _} = stream(ctx, wire, max_retries: 5)
    assert length(Clock.state(ctx.clock).calls) == 1
  end

  test "absent total budget preserves original transport waits and retry classification", ctx do
    wire = fn _request, _finch, acc, _callback, options ->
      Clock.call(ctx.clock, options)

      if length(Clock.state(ctx.clock).calls) == 1,
        do: {:error, %Req.TransportError{reason: :timeout}, acc},
        else: {:ok, acc}
    end

    assert {:ok, []} =
             ReqLLM.Streaming.Retry.stream(
               nil,
               :unused,
               [],
               fn _, acc -> acc end,
               [receive_timeout: 120_000, max_retries: 1],
               wire
             )

    assert Enum.map(Clock.state(ctx.clock).calls, fn {_, options} -> options end) == [
             [receive_timeout: 120_000],
             [receive_timeout: 120_000]
           ]
  end

  test "Req adapter waits shrink on each actual inner request and retain the caller's adapter",
       ctx do
    request =
      request(ctx, fn request ->
        Clock.call(ctx.clock, Map.to_list(request.options))
        Clock.advance(ctx.clock, 30)
        {request, %Req.TransportError{reason: :timeout}}
      end)

    assert {:error, %Req.TransportError{reason: :timeout}} = Req.request(request)

    assert Enum.map(Clock.state(ctx.clock).calls, fn {time, options} ->
             assert options[:receive_timeout] == options[:pool_timeout]
             assert options[:receive_timeout] == options[:request_timeout]
             {time, options[:receive_timeout]}
           end) == [{0, 100}, {30, 70}, {60, 40}, {90, 10}]
  end

  test "expired Req deadline returns a typed failure without invoking the adapter", ctx do
    Clock.advance(ctx.clock, 100)

    request =
      request(ctx, fn request ->
        Clock.call(ctx.clock, [])
        {request, Req.Response.new(status: 200)}
      end)

    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total, timeout: 100}} = Req.request(request)
    assert Clock.state(ctx.clock).calls == []
  end

  test "Req retry callback clamps retry-after and refuses an expired retry", ctx do
    request = request(ctx, fn request -> {request, Req.Response.new(status: 200)} end)
    response = Req.Response.new(status: 429, headers: [{"retry-after", "1"}])
    assert {:delay, 100} = ReqLLM.Step.Retry.should_retry?(request, response)
    Clock.advance(ctx.clock, 100)
    assert false == ReqLLM.Step.Retry.should_retry?(request, response)
  end

  test "stream options forward the exact captured deadline, without resetting it", ctx do
    options =
      ReqLLM.Streaming.FinchClient.stream_options(%{}, total_timeout_deadline: ctx.deadline)

    assert options[:total_timeout_deadline] == ctx.deadline
  end

  test "absolute cutoff is validated by the native schema and wins over a longer relative timeout" do
    absolute = System.system_time(:millisecond) + 100
    deadline = ReqLLM.TimeoutBudget.deadline(total_deadline_at: absolute, total_timeout: 10_000)
    assert deadline.expires_at <= System.monotonic_time(:millisecond) + 100
    assert ReqLLM.TimeoutBudget.remaining(deadline) in 0..100
    shorter = ReqLLM.TimeoutBudget.deadline(total_deadline_at: absolute, total_timeout: 20)
    assert ReqLLM.TimeoutBudget.remaining(shorter) in 0..20

    assert_raise NimbleOptions.ValidationError, fn ->
      ReqLLM.TimeoutBudget.deadline(total_deadline_at: "100")
    end

    assert_raise NimbleOptions.ValidationError, fn ->
      ReqLLM.TimeoutBudget.deadline(total_deadline_at: 0)
    end

    assert_raise NimbleOptions.ValidationError, fn ->
      ReqLLM.TimeoutBudget.request(Req.new(), :infinity, on_response_status: :invalid)
    end

    assert ReqLLM.Provider.Defaults.filter_req_opts(
             total_deadline_at: absolute,
             on_response_status: fn _ -> :ok end
           ) == []
  end

  test "an absolute cutoff already passed before library entry never opens a wire attempt", ctx do
    deadline = ReqLLM.TimeoutBudget.deadline(total_deadline_at: 1)

    req =
      Req.new(
        url: "https://unused.invalid",
        adapter: fn request ->
          Clock.call(ctx.clock, [])
          {request, Req.Response.new(status: 201)}
        end
      )

    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total}} =
             ReqLLM.TimeoutBudget.request(req, deadline)

    wire = fn _, _, acc, _, _ ->
      Clock.call(ctx.clock, [])
      {:ok, acc}
    end

    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total}, []} =
             stream(%{ctx | deadline: deadline}, wire, [])

    assert Clock.state(ctx.clock).calls == []
  end

  test "native absolute cutoff is carried through delayed stream and Req adapter entry", ctx do
    deadline =
      ReqLLM.TimeoutBudget.deadline(total_deadline_at: System.system_time(:millisecond) + 100)

    start = deadline.expires_at - 100
    Agent.update(ctx.clock, &%{&1 | now: start + 100})
    deadline = Map.put(deadline, :clock, {Clock, ctx.clock})

    req =
      request(%{ctx | deadline: deadline}, fn request ->
        Clock.call(ctx.clock, [])
        {request, Req.Response.new(status: 200)}
      end)

    assert {:error, %ReqLLM.Error.API.Timeout{}} = Req.request(req)

    wire = fn _, _, acc, _, _ ->
      Clock.call(ctx.clock, [])
      {:ok, acc}
    end

    assert {:error, %ReqLLM.Error.API.Timeout{}, []} =
             stream(%{ctx | deadline: deadline}, wire, [])

    assert Clock.state(ctx.clock).calls == []
  end

  test "stream observers receive actual retry and success statuses, never headers or content",
       ctx do
    owner = self()
    observer = fn fact -> send(owner, {:http_fact, fact}) end

    wire = fn _, _, acc, callback, _ ->
      Clock.call(ctx.clock, [])
      status = if length(Clock.state(ctx.clock).calls) == 1, do: 429, else: 206
      acc = callback.({:status, status}, acc)
      acc = callback.({:headers, [{"secret", "never-observed"}, {"retry-after", "0"}]}, acc)

      if status == 206 do
        acc = callback.({:data, "private-content"}, acc)
        {:ok, acc}
      else
        {:ok, acc}
      end
    end

    assert {:ok, _} = stream(ctx, wire, on_response_status: observer, max_retries: 1)
    assert_receive {:http_fact, %{status: 429} = fact1}
    assert map_size(fact1) == 1
    assert_receive {:http_fact, %{status: 206} = fact2}
    assert map_size(fact2) == 1
    refute_receive {:http_fact, _}
  end

  test "Req observer sees each actual status before decoding, preserving original response and errors",
       ctx do
    owner = self()
    observer = fn fact -> send(owner, {:http_fact, fact}) end

    req =
      request(ctx, fn request ->
        Clock.call(ctx.clock, [])
        status = if length(Clock.state(ctx.clock).calls) == 1, do: 429, else: 201

        {request,
         Req.Response.new(
           status: status,
           body: "private",
           headers: [{"secret", "hidden"}, {"retry-after", "0"}]
         )}
      end)

    req = Req.Request.put_private(req, :req_llm_response_status_observer, observer)
    assert {:ok, %Req.Response{status: 201, body: "private"}} = Req.request(req)
    assert_receive {:http_fact, %{status: 429} = fact1}
    assert map_size(fact1) == 1
    assert_receive {:http_fact, %{status: 201} = fact2}
    assert map_size(fact2) == 1
    refute_receive {:http_fact, _}
  end

  test "observer failures never alter streaming or nonstreaming transport behavior", ctx do
    for observer <- [
          fn _ -> raise "private" end,
          fn _ -> throw(:private) end,
          fn _ -> exit(:private) end
        ] do
      wire = fn _, _, acc, callback, _ -> {:ok, callback.({:status, 202}, acc)} end
      assert {:ok, [{:status, 202}]} = stream(ctx, wire, on_response_status: observer)

      req =
        Req.new(
          url: "https://unused.invalid",
          adapter: fn request -> {request, Req.Response.new(status: 204)} end
        )

      assert {:ok, %Req.Response{status: 204}} =
               ReqLLM.TimeoutBudget.request(req, :infinity, on_response_status: observer)
    end
  end

  test "status observer and captured cutoff survive Finch option assembly", ctx do
    observer = fn _ -> :ok end

    opts =
      ReqLLM.Streaming.FinchClient.stream_options(%{},
        total_timeout_deadline: ctx.deadline,
        on_response_status: observer
      )

    assert opts[:on_response_status] == observer
    assert opts[:total_timeout_deadline] == ctx.deadline
  end

  test "native nonstream total timer kills blocked I/O without invented status" do
    owner = self()

    req =
      Req.new(
        url: "https://unused.invalid",
        adapter: fn request ->
          send(owner, {:wire_entered, self()})

          receive do
            :continue -> {request, Req.Response.new(status: 200)}
          end
        end
      )

    task =
      Task.async(fn ->
        deadline =
          ReqLLM.TimeoutBudget.deadline(total_deadline_at: System.system_time(:millisecond) + 100)

        ReqLLM.TimeoutBudget.request(req, deadline,
          on_response_status: fn fact -> send(owner, {:http_fact, fact}) end
        )
      end)

    assert_receive {:wire_entered, wire_pid}
    monitor = Process.monitor(wire_pid)
    assert {:error, %ReqLLM.Error.API.Timeout{kind: :total}} = Task.await(task, 1_000)
    assert_receive {:DOWN, ^monitor, :process, ^wire_pid, :killed}
    refute_receive {:http_fact, _}
  end

  test "native stream cancellation stops attached I/O with no retry or made-up status", ctx do
    owner = self()
    model = %LLMDB.Model{id: "test", provider: :openai}

    {:ok, server} =
      ReqLLM.StreamServer.start_link(provider_mod: ReqLLM.Providers.OpenAI, model: model)

    task =
      Task.Supervisor.async_nolink(ReqLLM.TaskSupervisor, fn ->
        wire = fn _, _, acc, _, _ ->
          send(owner, {:stream_wire_entered, self()})

          receive do
            :continue -> {:ok, acc}
          end
        end

        stream(ctx, wire,
          max_retries: 10,
          on_response_status: fn fact -> send(owner, {:http_fact, fact}) end
        )
      end)

    assert_receive {:stream_wire_entered, wire_pid}
    assert :ok = ReqLLM.StreamServer.attach_http_task(server, wire_pid)
    assert :ok = ReqLLM.StreamServer.cancel(server)
    assert_receive {:DOWN, ref, :process, ^wire_pid, _}
    assert ref == task.ref
    refute_receive {:stream_wire_entered, _}
    refute_receive {:http_fact, _}
    if Process.alive?(server), do: GenServer.stop(server)
  end

  @tag :integration
  test "public generation carries validated absolute deadline and real success status through the native provider" do
    owner = self()

    body =
      Jason.encode!(%{
        id: "synthetic",
        object: "chat.completion",
        model: "gpt-4o-mini",
        choices: [
          %{index: 0, message: %{role: "assistant", content: "hello"}, finish_reason: "stop"}
        ],
        usage: %{prompt_tokens: 1, completion_tokens: 1, total_tokens: 2}
      })

    {port, server} = http_server(200, "application/json", body)
    absolute = System.system_time(:millisecond) + 5_000

    assert {:ok, response} =
             ReqLLM.Generation.generate_text(
               %LLMDB.Model{
                 id: "gpt-4o-mini",
                 provider: :openai,
                 extra: %{"wire" => %{"protocol" => "openai_chat"}}
               },
               "synthetic prompt",
               api_key: "synthetic",
               base_url: "http://127.0.0.1:#{port}/v1",
               total_deadline_at: absolute,
               on_response_status: fn fact -> send(owner, {:http_fact, fact}) end,
               max_retries: 0
             )

    assert ReqLLM.Response.text(response) == "hello"
    assert_receive {:http_fact, %{status: 200} = fact}
    assert map_size(fact) == 1
    assert :ok == Task.await(server)
  end

  @tag :integration
  test "public HTTP stream carries deadline and observer through Finch into actual status and output" do
    owner = self()

    body =
      "data: " <>
        Jason.encode!(%{
          id: "synthetic",
          object: "chat.completion.chunk",
          model: "gpt-4o-mini",
          choices: [
            %{index: 0, delta: %{role: "assistant", content: "hello"}, finish_reason: nil}
          ]
        }) <> "\n\ndata: [DONE]\n\n"

    {port, server} = http_server(206, "text/event-stream", body)
    absolute = System.system_time(:millisecond) + 5_000

    assert {:ok, response} =
             ReqLLM.Generation.stream_text(
               %LLMDB.Model{
                 id: "gpt-4o-mini",
                 provider: :openai,
                 extra: %{"wire" => %{"protocol" => "openai_chat"}}
               },
               "synthetic prompt",
               api_key: "synthetic",
               base_url: "http://127.0.0.1:#{port}/v1",
               total_deadline_at: absolute,
               on_response_status: fn fact -> send(owner, {:http_fact, fact}) end,
               max_retries: 0
             )

    assert ReqLLM.StreamResponse.text(response) == "hello"
    assert_receive {:http_fact, %{status: 206} = fact}
    assert map_size(fact) == 1
    assert :ok == Task.await(server)
  end

  @tag :integration
  test "LM Studio keeps its finite deadline at the transport boundary and reports actual status" do
    owner = self()

    body =
      "data: " <>
        Jason.encode!(%{
          id: "synthetic",
          object: "chat.completion.chunk",
          model: "local-model",
          choices: [
            %{index: 0, delta: %{role: "assistant", content: "hello"}, finish_reason: nil}
          ]
        }) <>
        "\n\ndata: [DONE]\n\n"

    {port, server} = http_server(200, "text/event-stream", body)
    absolute = System.system_time(:millisecond) + 5_000

    model =
      ReqLLM.model!(%{
        provider: :lmstudio,
        id: "local-model",
        provider_model_id: "publisher/model@q4:local",
        capabilities: %{chat: true, tools: %{enabled: true}}
      })

    assert {:ok, response} =
             ReqLLM.stream_text(model, "synthetic prompt",
               base_url: "http://127.0.0.1:#{port}/v1",
               total_deadline_at: absolute,
               on_response_status: fn fact -> send(owner, {:http_fact, fact}) end,
               max_retries: 0
             )

    assert ReqLLM.StreamResponse.text(response) == "hello"
    assert_receive {:http_fact, %{status: 200}}
    assert :ok == Task.await(server)
  end

  @tag :integration
  test "LM Studio finite cutoff cancels HTTP I/O waiting for stream body" do
    owner = self()
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_ip, port}} = :inet.sockname(listener)

    server =
      Task.async(fn ->
        try do
          {:ok, socket} = :gen_tcp.accept(listener, 2_000)
          {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)

          :ok =
            :gen_tcp.send(
              socket,
              "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n"
            )

          send(owner, :lmstudio_wire_opened)
          result = :gen_tcp.recv(socket, 0, 2_000)
          :gen_tcp.close(socket)
          result
        after
          :gen_tcp.close(listener)
        end
      end)

    model =
      ReqLLM.model!(%{
        provider: :lmstudio,
        id: "local-model",
        capabilities: %{chat: true, tools: %{enabled: true}}
      })

    absolute = System.system_time(:millisecond) + 200

    assert {:ok, response} =
             ReqLLM.stream_text(model, "synthetic prompt",
               base_url: "http://127.0.0.1:#{port}/v1",
               total_deadline_at: absolute,
               on_response_status: fn fact -> send(owner, {:http_fact, fact}) end,
               max_retries: 0
             )

    assert_receive :lmstudio_wire_opened
    assert_receive {:http_fact, %{status: 200}}
    error = assert_raise ReqLLM.Error.API.Stream, fn -> ReqLLM.StreamResponse.text(response) end
    assert %ReqLLM.Error.API.Timeout{kind: :total} = error.cause
    assert {:error, :closed} = Task.await(server, 2_000)
  end

  defp http_server(status, content_type, body) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_ip, port}} = :inet.sockname(listener)

    task =
      Task.async(fn ->
        try do
          {:ok, socket} = :gen_tcp.accept(listener, 2_000)
          {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)

          :ok =
            :gen_tcp.send(socket, [
              "HTTP/1.1 #{status} Test\r\ncontent-type: #{content_type}\r\ncontent-length: #{byte_size(body)}\r\nconnection: close\r\n\r\n",
              body
            ])

          :gen_tcp.close(socket)
        after
          :gen_tcp.close(listener)
        end
      end)

    {port, task}
  end

  test "caller death releases blocked nonstream I/O while its original absolute deadline remains live" do
    observer = self()
    absolute = System.system_time(:millisecond) + 10_000

    caller =
      spawn(fn ->
        request =
          Req.new(
            url: "https://unused.invalid",
            adapter: fn req ->
              send(observer, {:owned_adapter_entered, self()})

              receive do
                :continue -> {req, Req.Response.new(status: 200)}
              end
            end
          )

        deadline = ReqLLM.TimeoutBudget.deadline(total_deadline_at: absolute)
        ReqLLM.TimeoutBudget.request(request, deadline)
      end)

    owner_monitor = Process.monitor(caller)
    assert_receive {:owned_adapter_entered, request_pid}
    request_monitor = Process.monitor(request_pid)
    assert Process.alive?(request_pid)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^owner_monitor, :process, ^caller, :killed}
    assert_receive {:DOWN, ^request_monitor, :process, ^request_pid, :killed}, 1_000
    assert System.system_time(:millisecond) < absolute
  end

  test "finite request ownership preserves ordinary success and transport errors", ctx do
    for response <- [Req.Response.new(status: 202), %Req.TransportError{reason: :closed}] do
      request =
        Req.new(
          url: "https://unused.invalid",
          max_retries: 0,
          adapter: fn req -> {req, response} end
        )

      actual = ReqLLM.TimeoutBudget.request(request, ctx.deadline)

      if match?(%Req.Response{}, response),
        do: assert(actual == {:ok, response}),
        else: assert(actual == {:error, response})
    end
  end

  test "request guardian exits after successful I/O even while the caller remains alive" do
    owner = self()
    existing = MapSet.new(Task.Supervisor.children(ReqLLM.TaskSupervisor))

    caller =
      spawn(fn ->
        request =
          Req.new(
            url: "https://unused.invalid",
            adapter: fn req ->
              send(owner, {:cleanup_adapter_entered, self()})

              receive do
                :continue -> {req, Req.Response.new(status: 200)}
              end
            end
          )

        result =
          ReqLLM.TimeoutBudget.request(
            request,
            ReqLLM.TimeoutBudget.deadline(total_timeout: 10_000)
          )

        send(owner, {:cleanup_result, result})

        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)
    assert_receive {:cleanup_adapter_entered, request_pid}

    [guardian] =
      Task.Supervisor.children(ReqLLM.TaskSupervisor)
      |> MapSet.new()
      |> MapSet.difference(existing)
      |> MapSet.delete(request_pid)
      |> MapSet.to_list()

    guardian_monitor = Process.monitor(guardian)
    send(request_pid, :continue)
    assert_receive {:cleanup_result, {:ok, %Req.Response{status: 200}}}
    assert_receive {:DOWN, ^guardian_monitor, :process, ^guardian, :normal}
    assert Process.alive?(caller)
    send(caller, :finish)
  end

  for {advance, label} <- [{10_001, "expired"}, {9_999, "one millisecond remaining"}] do
    @advance advance
    @label label
    test "owner wait re-samples the original deadline after task startup: #{@label}" do
      controller = self()
      {:ok, state} = Agent.start_link(fn -> %{now: 0, owner_reads: 0} end)

      caller =
        spawn(fn ->
          receive do
            {:request, deadline} ->
              request =
                Req.new(
                  url: "https://unused.invalid",
                  adapter: fn req ->
                    send(controller, {:boundary_adapter_entered, self()})

                    receive do
                      :continue -> {req, Req.Response.new(status: 200)}
                    end
                  end
                )

              result = ReqLLM.TimeoutBudget.request(request, deadline)
              send(controller, {:boundary_result, result})

              receive do
                :finish -> :ok
              end
          end
        end)

      on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)

      deadline = %{
        timeout: 10_000,
        expires_at: 10_000,
        clock:
          {ReqLLM.TestSupport.RequestStartBoundaryClock,
           %{state: state, owner: caller, controller: controller}}
      }

      send(caller, {:request, deadline})
      assert_receive {:owner_wait_boundary, ^caller}, 1_000
      assert_receive {:boundary_adapter_entered, request_pid}, 1_000
      request_monitor = Process.monitor(request_pid)
      Agent.update(state, &%{&1 | now: @advance})
      send(caller, :release_owner_wait)

      assert_receive {:boundary_result,
                      {:error, %ReqLLM.Error.API.Timeout{kind: :total, timeout: 10_000}}},
                     1_000

      assert_receive {:DOWN, ^request_monitor, :process, ^request_pid, :killed}, 1_000
      assert Process.alive?(caller)
      send(caller, :finish)
    end
  end

  test "all streaming provider callbacks receive public options without the transport deadline" do
    model = ReqLLM.model!(%{provider: :openai, id: "synthetic"})
    context = ReqLLM.Context.new([ReqLLM.Context.user("synthetic")])

    for transport <- [:http, :websocket, :in_process] do
      absolute = System.system_time(:millisecond) + 5_000

      assert {:error, _} =
               ReqLLM.Streaming.start_stream(BoundaryProvider, model, context,
                 test_pid: self(),
                 test_transport: transport,
                 total_timeout: 5_000,
                 total_deadline_at: absolute
               )

      callbacks =
        if transport == :websocket,
          do: [:transport, transport],
          else: [:transport, :parser, transport]

      for callback <- callbacks do
        assert_receive {:callback_options, ^callback, opts}
        refute Keyword.has_key?(opts, :total_timeout_deadline)
        assert opts[:total_deadline_at] == absolute
        assert opts[:total_timeout] == 5_000
      end
    end
  end

  test "telemetry and diagnostics omit the internal transport deadline", ctx do
    model =
      ReqLLM.model!(%{
        provider: :openai,
        id: "synthetic",
        extra: %{"wire" => %{"protocol" => "openai_chat"}}
      })

    opts = [total_timeout_deadline: ctx.deadline, total_deadline_at: 10_000, total_timeout: 100]
    context = ReqLLM.Telemetry.new_context(model, opts, mode: :stream)
    refute Keyword.has_key?(context.original_opts, :total_timeout_deadline)
    assert context.original_opts[:total_deadline_at] == 10_000
    assert {:ok, diagnostic} = ReqLLM.RequestPlan.Diagnostic.build(model, :chat, opts)
    refute Jason.encode!(diagnostic) =~ "total_timeout_deadline"
  end

  defp stream(ctx, wire, opts) do
    opts =
      Keyword.merge(
        [
          receive_timeout: 120_000,
          pool_timeout: 120_000,
          request_timeout: 120_000,
          total_timeout_deadline: ctx.deadline
        ],
        opts
      )

    ReqLLM.Streaming.Retry.stream(
      nil,
      :unused,
      [],
      fn event, acc -> [event | acc] end,
      opts,
      wire
    )
  end

  defp request(ctx, adapter) do
    Req.new(
      url: "https://unused.invalid",
      adapter: adapter,
      receive_timeout: 120_000,
      finch: [pool_timeout: 120_000],
      max_retries: 10
    )
    |> ReqLLM.Step.Retry.attach()
    |> Req.Request.put_private(:req_llm_timeout_deadline, ctx.deadline)
    |> ReqLLM.Transport.attach()
  end
end
