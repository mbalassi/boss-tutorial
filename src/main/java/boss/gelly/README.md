#DataSet-Gelly BOSS Tutorial

## Goals

In this tutorial we are going to learn how to:
- read data from text files
- filter out records using the `filter` operator
- de-duplicate records using the `distinct` transformation of the `DataSet` API
- create a `Graph`
- run a Gelly library method and retrieving the result

## Data

We are going to use publicly available data from the The Koblenz Network Collection.
Specifically, we are going to analyze Linux kernel mailing list replies. The data represents the communication network of the linux kernel mailing list, where nodes are mailing list members and edges describe replies from one user to another. You can copy the data from the provided USB sticks.

## Create a graph and compute PageRank

### Step 1: Copy the edges file.
The file is quite small and you can easily open it and check it out on your laptop. You will see that the first line starts with `“%”` and contains no useful information. Each of the following lines represents one mailing list reply:

```
% asym positive
33	33	1	1138206644
33	28	1	1138211184
33	28	1	1138213453
28	2	1	1138215043
28	58	1	1138218253
```

The first two fields are user Ids, the third field is the edge weight (this is 1 throughout the file) and the last field is a timestamp.

### Step 2: Load the data in a Flink DataSet.
We will use Flink’s `readAsCsv` method to read the replies data and create edges. 
Flink’s CSV reader has convenient methods that let us:
- Define the field delimiter. In our case this is the tab character `‘\t’` that separates the user Ids. The line delimiter is by default the new line character, so we do not need to set it.
- Ignore comments; lines that start with a certain character. Using this feature, we can instruct Flink to ignore the first line of the file.
- Ignore some of the file columns. Using this feature, we are going to ignore the timestamp.
- Define the field types. We will read the user Ids as `Long`s and the edge weight as a `Double`.

```
DataSet<Tuple3<Long, Long, Double>> replies = 
				env.readCsvFile("/path/to/the/input/file")
				.fieldDelimiter(...)
				.ignoreComments(...)
				.includeFields(...)
				.types(...);
```

### Step 3: Filter out self-replies
Next, we are going to filter out self-replies, i.e. records that have the same sender and receiver ID:

```
DataSet<Tuple3<Long, Long, Double>> filteredReplies = replies.filter(new SelfRepliesFilter());

…

public static final class SelfRepliesFilter implements FilterFunction<Tuple3<Long, Long, Double>> {

	@Override
	public boolean filter(Tuple3<Long, Long, Double> tuple) {
	// keep only the tuples where the from-ID is different from the to-ID 
			return ...
		}
	}
```

### Step 4: Keep only a single reply for the same pair of users
In the dataset, each reply between a couple of users appears as a separate tuple. That means that if user a has replied 5 times to user b, then there exist 5 different `<a, b, 1>` tuples in our dataset. For each interaction between between the same pair of users, we are going to keep only a single tuple.
Use `DataSet`’s `distinct()` method to only keep one tuple for the same pair of user Ids:

```
DataSet<Tuple3<Long, Long, Double>> distinctReplies = ...
```

### Step 5: Create a Graph
We can now use the replies dataset as edges input and create a `Graph` with Gelly’s `fromTupleDataSet()` method. In this method, we can also provide a mapper to create the vertex values. The mapper gives us the vertex Id as a parameter and lets us set the vertex value to an arbitrary value. This allows us to create the graph and initialize the vertex values in one step. Here, we will simply set the vertex values to 1.0:

```
Graph<Long, Double, Double> graph = Graph.fromTupleDataSet(..., new InitVertices(), env);

…

public static final class InitVertices implements MapFunction<Long, Double> {

@Override
	public Double map(Long vertexId) {
		return ...
	}
}
```

### Step 6: Run PageRank
We can now simply use `Graph`’s `run()` method to call Gelly’s `PageRank` library method.
Set the iterations parameter to 10 and the dampening factor (beta) to 0.85.
Then, we can inspect the results by retrieving and printing the vertices.

```
DataSet<Vertex<Long, Double>> ranks = graph.run(...).getVertices();

ranks.print();
```
