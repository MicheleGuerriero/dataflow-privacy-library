package financial.example;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import financial.example.datatypes.FinancialTransaction;
import financial.example.datatypes.TopConsumersCount;
import financial.example.datatypes.TotalExpense;
import financial.example.datatypes.TransactionsCount;
import financial.example.functions.TopConsumersCounter;
import financial.example.functions.TotalExpenseCalculator;
import financial.example.functions.TransactionCounter;
import financial.example.functions.TransactionParser;

public class FinancialTransactionJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> input = env.socketTextStream("localhost", 9999);

		DataStream<FinancialTransaction> s1 = input.map(new TransactionParser())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<FinancialTransaction>() {

					@Override
					public long extractAscendingTimestamp(FinancialTransaction element) {
						return element.getEventTime();
					}
				});

		DataStream<TransactionsCount> s2 = s1.keyBy("dataSubject").timeWindow(Time.seconds(10))
				.apply(new TransactionCounter());

		DataStream<TotalExpense> s3 = s1.keyBy("dataSubject").timeWindow(Time.seconds(10))
				.apply(new TotalExpenseCalculator());

		DataStream<TopConsumersCount> s4 = s3.timeWindowAll(Time.minutes(1)).apply(new TopConsumersCounter());

		s2.writeAsText("/home/utente/eclipse-workspace/library/results/s2.txt", WriteMode.OVERWRITE);

		s3.writeAsText("/home/utente/eclipse-workspace/library/results/s3.txt", WriteMode.OVERWRITE);

		s4.writeAsText("/home/utente/eclipse-workspace/library/results/s4.txt", WriteMode.OVERWRITE);

		env.execute();

	}

}
