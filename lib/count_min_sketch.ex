defmodule CountMinSketch do
  use GenServer

  @width 3
  @length 64

  ### interface ###

  def start_link do
    GenServer.start_link(__MODULE__, [@width, @length])
  end

  @doc """
  Register a value in the sketch
  """
  def add(pid, value) do
    GenServer.cast(pid, {:add, value})
  end

  @doc """
  View the state of the sketch
  """
  def get(pid) do
    GenServer.call(pid, :get)
  end

  @doc """
  Get the frequency of a value in the sketch
  """
  def get_frequency(pid, value) do
    GenServer.call(pid, {:get_frequency, value})
  end

  ### callbacks ###

  def init([width, length]) do
    sketch = build_initial_sketch(width, length)

    {:ok, sketch}
  end

  def handle_cast({:add, value}, state) do
    {:noreply, update_sketch(state, value)}
  end

  def handle_call(:get, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:get_frequency, value}, _from, state) do
    {:reply, get_min(state, value), state}
  end

  ### priv/impl ###

  defp build_initial_sketch(width, length) do
    Enum.reduce(0..width - 1, %{}, fn (w, row_acc) ->
      row = Enum.reduce(0..length - 1, %{}, fn (l, col_acc) ->
        Map.update(col_acc, l, 1, fn -> 1 end)
      end)

      Map.update(row_acc, w, row, fn -> row end)
    end)
  end

  defp update_sketch(sketch, value) do
    Enum.reduce(sketch, sketch, fn({row_num, _row}, acc) ->
      {_new_val, new_sketch} = get_and_update_in(
        acc,
        [row_num, hash(value)],
        &{&1, &1 + 1}
      )

      new_sketch
    end)
  end

  defp get_min(sketch, value) do
    sketch
    |> Enum.map(fn {_row_num, row} -> Map.fetch!(row, hash(value)) end)
    |> Enum.min
  end

  defp hash(value) do
    :erlang.phash2(value, @length)
  end
end
