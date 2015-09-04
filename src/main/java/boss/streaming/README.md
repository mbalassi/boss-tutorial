#DataStream BOSS Tutorial

## Goals

In this tutorial we are going to learn how to:
- read streaming data from a socket
- parse and integrate multiple data sources
- compute basic aggregations on data streams
- introduce stream windows and computations on them
- connect and share state between streams

## Exercise

In this exercise we are processing temperature data measured in cities to demonstrate the Flink features outlined in the goals section. For encapsulating the temperature data we are using the `Temp` class.

## Step 1: Read data from a socket and integrate multiple sources
Let us use a socket source, so that data can be interactively typed in for the topology. Open up a terminal and execute `nc -lk 9999`. Any text going to this terminal is going to be sent as input for your Flink topology.

Flink can read this stream using `StreamExecutionEnvironment#socketTextStream`. For some static input we can also use `StreamExecutionEnvironment#fromElements`. For unifying and parsing the data one may use `DataStream#connect` and `DataStream#map`.

## Step 2: Rolling aggregates and Stateful computation

Let us compute rolling aggregation on the data stream, for the available functions check out the relevant section of the documentation. [1]

To implement aggregations with a persistent state see [2].

[1] https://ci.apache.org/projects/flink/flink-docs-master/apis/streaming_guide.html#aggregations
[2] https://ci.apache.org/projects/flink/flink-docs-master/apis/streaming_guide.html#stateful-computation

## Step 3: Windowing

Apply windowing to the stream by time and delta, see [3].

[3] https://ci.apache.org/projects/flink/flink-docs-master/apis/streaming_guide.html#window-operators

## Step 4: Share state between streams

Use the `DataStream#connect` feature to share state between streams. To process the resulting `ConnectedDataStream` see [4].

[4] https://ci.apache.org/projects/flink/flink-docs-master/apis/streaming_guide.html#co-operators

## Step 5: Iterations and global state

To demonstrate a possible use case of streaming iterations a global partitioned is implemented for you in step 5. Check out the code! :)
