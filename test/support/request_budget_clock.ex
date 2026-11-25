defmodule ReqLLM.TestSupport.RequestBudgetClock do
  @moduledoc "Deterministic clock recording synthetic request waits and attempts."

  def start, do: Agent.start_link(fn -> %{now: 0, waits: [], calls: []} end)
  def now(clock), do: Agent.get(clock, & &1.now)

  def sleep(clock, delay),
    do: Agent.update(clock, &%{&1 | now: &1.now + delay, waits: &1.waits ++ [delay]})

  def advance(clock, delay), do: Agent.update(clock, &%{&1 | now: &1.now + delay})

  def call(clock, options),
    do: Agent.update(clock, &%{&1 | calls: &1.calls ++ [{&1.now, options}]})

  def state(clock), do: Agent.get(clock, & &1)
  def deadline(clock), do: %{expires_at: 100, timeout: 100, clock: {__MODULE__, clock}}
end

defmodule ReqLLM.TestSupport.RequestStartBoundaryClock do
  @moduledoc "Event-controlled clock holding the request owner across task startup."

  def now(%{owner: owner, controller: controller, state: state}) do
    caller = self()

    read =
      Agent.get_and_update(state, fn data ->
        count = if caller == owner, do: data.owner_reads + 1, else: data.owner_reads
        {count, %{data | owner_reads: count}}
      end)

    if caller == owner and read == 2 do
      send(controller, {:owner_wait_boundary, caller})

      receive do
        :release_owner_wait -> :ok
      end
    end

    Agent.get(state, & &1.now)
  end
end
