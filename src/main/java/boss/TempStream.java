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

import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

public class TempStream {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<String> tempSource1 = env.fromElements("BP,20", "HI,300");
		DataStream<String> tempSource2 = env.socketTextStream("localhost", 9999);

		DataStream<String> tempStrings = tempSource1.union(tempSource2);

		DataStream<Temp> temps = tempStrings.flatMap(new TempParser());
		GroupedDataStream<Temp> tempsByCity = temps.groupBy("city");

		DataStream<Temp> rollingMaxTemp = tempsByCity.maxBy("temp");
		DataStream<Temp> rollingAvgTemp = tempsByCity.map(new RollingAvg());

		DataStream<Pop> pops = env.socketTextStream("localhost", 9998).flatMap(new PopParser());

		DataStream<CityInfo> cityInfo = rollingAvgTemp.connect(pops).groupBy("city", "city").flatMap(new Enricher());

		DataStream<Temp> windowMin = tempsByCity.window(Count.of(3)).every(Count.of(2)).minBy("temp").flatten();

		rollingMaxTemp.addSink(new PrintWithPrefix("Max"));
		rollingAvgTemp.addSink(new PrintWithPrefix("Avg"));
		windowMin.addSink(new PrintWithPrefix("WindowMin"));
		cityInfo.addSink(new PrintWithPrefix("PopWithAvg"));

		env.execute();

	}

	public static class Temp {
		public String city;
		public Double temp;

		public Temp() {
			city = "MISSING";
			temp = 0.0;
		}

		public Temp(String city, Double temp) {
			this.city = city;
			this.temp = temp;
		}

		public String toString() {
			return "(" + city + ", " + temp + ")";
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

	public static class TempParser implements FlatMapFunction<String, Temp> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String in, Collector<Temp> out) throws Exception {
			try {
				String[] split = in.split(",");
				out.collect(new Temp(split[0], Double.valueOf(split[1])));
			} catch (Exception e) {
			}
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

	public static class PopParser implements FlatMapFunction<String, Pop> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String in, Collector<Pop> out) throws Exception {
			try {
				String[] split = in.split(",");
				out.collect(new Pop(split[0], Long.valueOf(split[1])));
			} catch (Exception e) {
			}
		}

	}

	public static class RollingAvg extends RichMapFunction<Temp, Temp> {

		private static final long serialVersionUID = 1L;

		private OperatorState<Integer> count;
		private OperatorState<Double> sum;

		@Override
		public Temp map(Temp value) throws Exception {
			count.update(count.value() + 1);
			sum.update(sum.value() + value.temp);
			return new Temp(value.city, sum.value() / count.value());
		}

		@Override
		public void open(Configuration conf) throws IOException {
			RuntimeContext ctx = getRuntimeContext();
			count = ctx.getOperatorState("count", 0, true);
			sum = ctx.getOperatorState("sum", 0., true);
		}

	}

	public static class Enricher extends RichCoFlatMapFunction<Temp, Pop, CityInfo> {

		private static final long serialVersionUID = 1L;

		private OperatorState<HashMap<String, Double>> avgTemps;

		@Override
		public void flatMap1(Temp temp, Collector<CityInfo> out) throws Exception {
			HashMap<String, Double> currAvgs = avgTemps.value();
			currAvgs.put(temp.city, temp.temp);
			avgTemps.update(currAvgs);
		}

		@Override
		public void flatMap2(Pop pop, Collector<CityInfo> out) throws Exception {
			Double currAvg = avgTemps.value().get(pop.city);
			if (currAvg != null) {
				out.collect(new CityInfo(pop.city, currAvg, pop.population));
			}
		}

		public void open(Configuration conf) throws IOException {
			this.avgTemps = getRuntimeContext().getOperatorState("avg", new HashMap<String, Double>(), false);
		}

	}

}
