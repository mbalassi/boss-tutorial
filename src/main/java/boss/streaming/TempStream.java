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

package boss.streaming;

import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

public class TempStream {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		// Obtain the environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		//================================= Exercise 1 =====================================

		// Read temperature data from various sources
		// DataStream<String> tempSource1 = env.fromElements("BP,20", "HI,30");
		// DataStream<String> tempSource2 = env.socketTextStream("localhost", 9999);

		// Create a unified stream of input strings
		// DataStream<String> tempStrings = ...;

		// Parse the raw source data and group by city
		// DataStream<Temp> temps = ...;
		// GroupedDataStream<Temp> tempsByCity = ...

		//================================= Exercise 2 =====================================
		
		// Compute rolling max
		// DataStream<Temp> rollingMaxTemp = ...
		// Compute rolling average using a stateful map (fill the map)
		// DataStream<Temp> rollingAvgTemp = tempsByCity.map(new RollingAvg());
		
		// rollingMaxTemp.addSink(new PrintWithPrefix("Max"));
		// rollingAvgTemp.addSink(new PrintWithPrefix("Avg"));
		
		//================================= Exercise 3 =====================================
		
		// Apply a window minimum on the temperatures for each city in the last 5 seconds
		// DataStream<Temp> windowMin = tempsByCity.window(...).every(...)..
		
		// windowMin.addSink(new PrintWithPrefix("WindowMin"));	
		
		// Generate an alert when a city is experiencing a temperature difference of 20 degrees
		// DataStreamSink<String> alerts = tempsByCity.window(new Delta<Temp>(...));

		//================================= Exercise 4 =====================================

		// Read population data from a socket and parse it
		// DataStream<Pop> pops = env.socketTextStream("localhost", 9998).flatMap(new PopParser());

		// Enrich the incoming population data with the current rolling avg temperature
		// The Enricher skeleton should give some ideas here...
		// DataStream<CityInfo> cityInfo = ...
		
		// cityInfo.addSink(new PrintWithPrefix("PopWithAvg"));
		
		//================================= Exercise 5 =====================================

//		DataStream<Diff> cityPairs = env.socketTextStream("localhost", 9997).map(new MapFunction<String, Diff>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Diff map(String arg0) throws Exception {
//				String[] split = arg0.split(",");
//				return new Diff(split[0], split[1]);
//			}
//		}).partitionByHash(0);
//
//		IterativeDataStream<Diff> it = cityPairs.iterate();
//		DataStream<Diff> step = rollingAvgTemp.partitionByHash("city").connect(it).flatMap(new TempDiffCoOp());
//
//		it.closeWith(step.filter(new FilterFunction<Diff>() {
//
//			@Override
//			public boolean filter(Diff d) throws Exception {
//				return !d.isSecondMatched();
//			}
//		}).partitionByHash(1));
//
//		DataStream<Diff> diffs = step.filter(new FilterFunction<TempStream.Diff>() {
//
//			@Override
//			public boolean filter(Diff d) throws Exception {
//				return d.isSecondMatched();
//			}
//		});

		// diffs.addSink(new PrintWithPrefix("Diff"));

		// Execute the streaming program
		env.execute();

	}

	public static class Temp {
		public String city;
		public Double temperature;

		public Temp() {
			city = "MISSING";
			temperature = 0.0;
		}

		public Temp(String city, Double temp) {
			this.city = city;
			this.temperature = temp;
		}

		public String toString() {
			return "(" + city + ", " + temperature + ")";
		}
	}

	public static class RollingAvg extends RichMapFunction<Temp, Temp> {
	
		private static final long serialVersionUID = 1L;
	
		// We need to store the current count and sum for each city as
		// partitioned state
		private OperatorState<Integer> count;
		private OperatorState<Double> sum;
	
		@Override
		public Temp map(Temp value) throws Exception {
			// update state and output the current avg
			return null;
		}
	
		@Override
		public void open(Configuration conf) throws IOException {
			// Get the partitioned states from the runtime context
			RuntimeContext ctx = getRuntimeContext();
			count = ctx.getOperatorState("count", 0, true);
			sum = ctx.getOperatorState("sum", 0., true);
		}
	
	}

	@SuppressWarnings("rawtypes")
	public static class PrintWithPrefix implements SinkFunction {

		private static final long serialVersionUID = 1L;
		private String prefix;

		public PrintWithPrefix(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public void invoke(Object in) throws Exception {
			System.out.println(prefix + " - " + in);
		}

	}

	public static class Pop {
		public String city;
		public Long population;
	
		public Pop() {
			city = "MISSING";
			population = 0L;
		}
	
		public Pop(String city, Long pop) {
			this.city = city;
			this.population = pop;
		}
	
		public String toString() {
			return "(" + city + ", " + population + ")";
		}
	}

	public static class CityInfo {
		public String city;
		public Double avgTemp;
		public Long population;
	
		public CityInfo() {
			city = "MISSING";
			population = 0L;
			avgTemp = 0.0;
		}
	
		public CityInfo(String city, Double avgTemp, Long pop) {
			this.city = city;
			this.avgTemp = avgTemp;
			this.population = pop;
		}
	
		public String toString() {
			return "(" + city + ", " + avgTemp + ", " + population + ")";
		}
	}

	public static class PopParser implements FlatMapFunction<String, Pop> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String in, Collector<Pop> out) throws Exception {
			try {
				String[] split = in.split(",");
				out.collect(new Pop(split[0], Long.valueOf(split[1])));
			} catch (Exception e) {
				// We only output successfully parsed records
			}
		}

	}

	public static class Enricher extends RichCoFlatMapFunction<Temp, Pop, CityInfo> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap1(Temp temp, Collector<CityInfo> out) throws Exception {
			// Update the current avg for the city
		}

		@Override
		public void flatMap2(Pop pop, Collector<CityInfo> out) throws Exception {
			// Output the the city info with the most recent avg temperature
		}

		@Override
		public void open(Configuration conf) throws IOException {
			// initialize states? ;)
		}

	}

	public static class Diff extends Tuple4<String, String, Double, Integer> {

		private static final long serialVersionUID = 1L;

		public Diff() {
			super();
		}
	
		public Diff(String f0, String f1) {
			super(f0, f1, 0.0, 0);
		}
	
		public Diff match(Double temp) {
			if (f3 == 0) {
				f2 = temp;
			} else if (f3 == 1) {
				f2 -= temp;
			}
			f3 += 1;
			return this;
		}
	
		public boolean isFirstMatched() {
			return f3 > 0;
		}
	
		public boolean isSecondMatched() {
			return f3 > 1;
		}
	
		public String getFirst() {
			return f0;
		}
	
		public String getSecond() {
			return f1;
		}
	
		public Tuple2<String, String> getCities() {
			return Tuple2.of(f0, f1);
		}
	
		public Double getDiff() {
			return f2;
		}
	
		public String toString() {
			return getCities().toString() + " - " + getDiff();
		}
	}

	public static class TempDiffCoOp extends RichCoFlatMapFunction<Temp, Diff, Diff> {
	
		// Store the most recent avg temperature for each city as state
		private OperatorState<HashMap<String, Double>> avgTemps;
	
		@Override
		public void flatMap1(Temp temp, Collector<Diff> out) throws Exception {
			HashMap<String, Double> currAvgs = avgTemps.value();
			currAvgs.put(temp.city, temp.temperature);
			avgTemps.update(currAvgs);
		}
	
		@Override
		public void flatMap2(Diff diff, Collector<Diff> out) throws Exception {
			if (!diff.isFirstMatched()) {
				if (avgTemps.value().containsKey(diff.getFirst())) {
					out.collect(diff.match(avgTemps.value().get(diff.getFirst())));
				}
			} else {
				if (avgTemps.value().containsKey(diff.getSecond())) {
					out.collect(diff.match(avgTemps.value().get(diff.getSecond())));
				}
			}
		}
	
		public void open(Configuration conf) throws IOException {
			// Get local state from the runtime context
			this.avgTemps = getRuntimeContext().getOperatorState("avg", new HashMap<String, Double>(), false);
		}
	
	}

}
