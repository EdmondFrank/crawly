defmodule Crawly.Middlewares.BloomUniqueRequest do
  @moduledoc """
  Avoid scheduling requests for the same pages by bloom filter
  """
  require Logger

  def run(request, state, opts \\ []) do

    opts = Enum.into(opts, %{capacity: nil, false_positive: nil})

    capacity = Map.get(opts, :capacity, 1_000_000)
    false_positive = Map.get(opts, :false_positive, 0.001)
    ignore_list = Map.get(opts, :ignore_list, [])

    if request.url in ignore_list do
      {request, state}
    else
      unique_request_bloom_filter =
        Map.get(state, :unique_request_bloom_filter, init_bloom(capacity, false_positive))

      if :bloom.check(unique_request_bloom_filter, request.url) do
        Logger.debug(
          "Dropping request by bloom filter: #{request.url}, as it's already processed"
        )

        {false, state}
      else
        :bloom.set(unique_request_bloom_filter, request.url)

        {request, state}
      end
    end
  end

  defp init_bloom(capacity, false_positive) do
    {:ok, bloom} = :bloom.new_optimal(capacity, false_positive)
    bloom
  end
end
