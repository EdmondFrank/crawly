defmodule Crawly.RequestsStorage.Worker do
  @moduledoc """
  Requests Storage, is a module responsible for storing requests for a given
  spider.

  Automatically filters out already seen requests (uses `fingerprints` approach
  to detect already visited pages).

  Pipes all requests through a list of middlewares, which do pre-processing of
  all requests before storing them
  """
  require Logger

  use GenServer

  defstruct requests: nil, count: 0, working: 0, spider_name: nil, crawl_id: nil

  alias Crawly.RequestsStorage.Worker

  @doc """
  Store individual request or multiple requests
  """
  @spec store(Crawly.spider(), Crawly.Request.t() | Qex.t()) :: :ok
  def store(pid, %Crawly.Request{} = request), do: do_call(pid, {:store, request})

  def store(pid, %Qex{} = requests) do
    do_call(pid, {:store, requests})
  end

  def store(pid, requests) when is_list(requests) do
    do_call(pid, {:store, requests})
  end

  @doc """
  Pop a request out of requests storage
  """
  @spec pop(pid()) :: Crawly.Request.t() | nil
  def pop(pid) do
    do_call(pid, :pop)
  end

  @doc """
  Increase working requests count
  """
  @spec inc(pid()) :: non_neg_integer()
  def inc(pid) do
    do_call(pid, :inc)
  end

  @doc """
  Decrease working requests count
  """
  @spec dec(pid()) :: non_neg_integer()
  def dec(pid) do
    do_call(pid, :dec)
  end

  @doc """
  Get statistics from the requests storage
  """
  @spec stats(pid()) :: {:stored_requests, non_neg_integer()}
  def stats(pid) do
    do_call(pid, :stats)
  end

  @doc """
  Returns all scheduled requests (used for some sort of preview)
  """
  @spec requests(pid()) :: {:requests, [Crawly.Request.t()]}
  def requests(pid), do: do_call(pid, :requests)

  @doc """
  Get working requests count from the requests storage
  """
  @spec working_stats(pid()) :: {:working_requests, non_neg_integer()}
  def working_stats(pid) do
    do_call(pid, :working_stats)
  end

  def start_link(spider_name, crawl_id) do
    GenServer.start_link(__MODULE__, [spider_name, crawl_id])
  end

  def init([spider_name, crawl_id]) do
    Logger.metadata(spider_name: spider_name, crawl_id: crawl_id)

    Logger.debug(
      "Starting requests storage worker for #{inspect(spider_name)}..."
    )

    {:ok, %Worker{requests: Qex.new, spider_name: spider_name, crawl_id: crawl_id}}
  end

  # Store the given request
  def handle_call({:store, %Crawly.Request{} = request}, _from, state) do
    new_state = pipe_request(request, state)
    {:reply, :ok, new_state}
  end

  # Store the given requests
  def handle_call({:store, requests}, _from, state) do
    new_state = Enum.reduce(requests, state, &pipe_request/2)
    {:reply, :ok, new_state}
  end

  # Get current request from the storage
  def handle_call(:pop, _from, state) do
    %Worker{requests: requests, count: cnt} = state

    {request, rest, new_cnt} =
      case Qex.pop(requests) do
        {:empty, _} -> {nil, requests, 0}
        {{:value, request}, rest} -> {request, rest, cnt - 1}
      end

    {:reply, request, %Worker{state | requests: rest, count: new_cnt}}
  end

  # Increase current working requests count
  def handle_call(:inc, _from, state) do
    new_working = state.working + 1

    {:reply, new_working, %Worker{state | working: new_working}}
  end

  # Decrease current working requests count
  def handle_call(:dec, _from, state) do
    new_working = if state.working > 0, do: state.working - 1, else: 0

    {:reply, new_working, %Worker{state | working: new_working}}
  end

  def handle_call(:stats, _from, state) do
    {:reply, {:stored_requests, state.count}, state}
  end

  def handle_call(:requests, _from, state) do
    {:reply, {:requests, state.requests}, state}
  end

  def handle_call(:working_stats, _from, state) do
    {:reply, {:working_requests, state.working}, state}
  end

  defp do_call(pid, command) do
    GenServer.call(pid, command)
  catch
    error, reason ->
      Logger.error(Exception.format(error, reason, __STACKTRACE__))
  end

  defp pipe_request(request, state) do
    case Crawly.Utils.pipe(request.middlewares, request, state) do
      {false, new_state} ->
        new_state

      {new_request, new_state} ->
        # Process request here....
        %{
          new_state
          | count: state.count + 1,
            requests: Qex.push_front(state.requests, new_request)
        }
    end
  end
end
