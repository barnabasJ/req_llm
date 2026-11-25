defmodule ReqLLM.ResponseStatus do
  @moduledoc false

  def notify(nil, _status), do: :ok

  def notify(observer, status) when is_integer(status) do
    observer.(%{status: status})
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end
