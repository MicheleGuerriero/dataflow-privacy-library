package financial.example;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import example1.utils.StreamMerger;
import financial.example.datatypes.FinancialTransaction;
import financial.example.datatypes.TopConsumersCount;
import financial.example.datatypes.TotalExpense;
import financial.example.datatypes.TransactionsCount;
import financial.example.functions.TopConsumersCounter;
import financial.example.functions.TotalExpenseCalculator;
import financial.example.functions.TransactionCounter;

import java.util.*;

import org.apache.flink.api.java.tuple.Tuple2;

public class FinancialTransactionJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<Tuple2<FinancialTransaction, Long>> workload = new ArrayList<Tuple2<FinancialTransaction, Long>>();
		
		// wrong workload in processing time
		workload.add(new Tuple2(new FinancialTransaction("t1", "Bob", 50, "Mary", 100050L), 60L));
		workload.add(new Tuple2(new FinancialTransaction("t2", "Mary", 100, "Paul", 100110L), 280L));
		workload.add(new Tuple2(new FinancialTransaction("t3", "Bob", 100, "Paul", 100390L), 270L));
		workload.add(new Tuple2(new FinancialTransaction("t4", "Paul", 200, "Mary", 100660L), 220L));
		workload.add(new Tuple2(new FinancialTransaction("t5", "Bob", 150, "Mary", 100880L), 130L));
		workload.add(new Tuple2(new FinancialTransaction("t6", "Mary", 50, "Paul", 101010L), 510L));
		workload.add(new Tuple2(new FinancialTransaction("t7", "Paul", 70, "Bob", 101520L), 380L));
		workload.add(new Tuple2(new FinancialTransaction("t8", "Bob", 120, "Mary", 101900L), 470L));
		workload.add(new Tuple2(new FinancialTransaction("t9", "Mary", 500, "Paul", 102370L), 330L));
		workload.add(new Tuple2(new FinancialTransaction("t10", "Bob", 130, "Mary", 102700L), 250L));
		workload.add(new Tuple2(new FinancialTransaction("t11", "Paul", 300, "Bob", 102950L), 250L));
		workload.add(new Tuple2(new FinancialTransaction("t12", "Mary", 150, "Bob", 103200L), 400L));
		workload.add(new Tuple2(new FinancialTransaction("t13", "Bob", 70, "Paul", 103600L), 300L));
		workload.add(new Tuple2(new FinancialTransaction("t14", "Mary", 230, "Paul", 103900L), 400L));
		workload.add(new Tuple2(new FinancialTransaction("t15", "Bob", 550, "Paul", 104300L), 0L));

		DataStream<FinancialTransaction> s1 = env.addSource(new FinancialTransactionFixedSource(0, 0, workload))
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
