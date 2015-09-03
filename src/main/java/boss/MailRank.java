/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package boss;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.PageRank;

/**
 * This is a skeleton code for the DataSet/Gelly tutorial of the BOSS workshop. 
 *
 * <p>
 * This program:
 * <ul>
 * <li>reads a list of user interactions in a mailing list
 * <li>filters out self-replies
 * <li>de-duplicates replies between the same user pair
 * <li>creates a graph from the filtered data
 * <li>computes the Page Rank for each user, using Gelly's PageRank algorithm
 * <li>prints the result to stdout
 * </ul>
 *
 */
public class MailRank {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Step #2: Load the data in a DataSet 
		DataSet<Tuple3<Long, Long, Double>> replies = env.readCsvFile("/path/to/the/input/file")
				.fieldDelimiter(...)
				.ignoreComments(...)
				.includeFields(...)
				.types(...);

		// Step #3: Filter out self-replies
		DataSet<Tuple3<Long, Long, Double>> filteredReplies = replies.filter(new SelfRepliesFilter());


		// Step #4: De-duplicate replies between the same user-pair
		DataSet<Tuple3<Long, Long, Double>> distinctReplies = ...

		// Step #5: Create a Graph and initialize vertex values to 1.0
		Graph<Long, Double, Double> graph = Graph.fromTupleDataSet(..., new InitVertices(), env);

		// Step #6: Run PageRank
		DataSet<Vertex<Long, Double>> ranks = graph.run(...).getVertices();

		// Print the result
		ranks.print();
	}

	//
	// 	User Functions
	//

	/**
	 * Filters-out self-replies from the input data
	 */
	@SuppressWarnings("serial")
	public static final class SelfRepliesFilter implements FilterFunction<Tuple3<Long, Long, Double>> {

		@Override
		public boolean filter(Tuple3<Long, Long, Double> tuple) {
		// keep only the tuples where the from-ID is different from the to-ID 
				return ...
			}
		}


	/**
	 * Initializes vertex values with the value 1.0
	 */
	@SuppressWarnings("serial")
	public static final class InitVertices implements MapFunction<Long, Double> {

		@Override
		public Double map(Long vertexId) {
			return ...
		}
	}
}
