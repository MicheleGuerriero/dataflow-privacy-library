package financial.example;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.yaml.snakeyaml.Yaml;

import example1.utils.StreamMerger;
import financial.example.datatypes.FinancialTransaction;
import financial.example.datatypes.TopConsumersCount;
import financial.example.datatypes.TotalExpense;
import financial.example.datatypes.TransactionsCount;
import financial.example.functions.TopConsumersCounter;
import financial.example.functions.TotalExpenseCalculator;
import financial.example.functions.TransactionCounter;
import financial.example.functions.TransactionParser;
import it.deib.polimi.diaprivacy.library.GeneralizationFunction;
import it.deib.polimi.diaprivacy.library.PrivacyContextFixedSource;
import it.deib.polimi.diaprivacy.library.ProtectedStream;
import it.deib.polimi.diaprivacy.model.ApplicationDataStream;
import it.deib.polimi.diaprivacy.model.ApplicationPrivacy;
import it.deib.polimi.diaprivacy.model.DSEP;
import it.deib.polimi.diaprivacy.model.PrivacyContext;
import it.deib.polimi.diaprivacy.model.VCP;

public class PrivateFinancialTransactionJob {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(0);

		/// privacy init
		Yaml yaml = new Yaml();

		String content = FileUtils.readFileToString(
				new File("/home/utente/eclipse-workspace/library/src/main/java/financial/example/privacy-config.yml"),
				"UTF-8");

		ApplicationPrivacy app = yaml.loadAs(content, ApplicationPrivacy.class);

		DataStreamSource<PrivacyContext> contextStream = env
				.addSource(new PrivacyContextFixedSource(0, 2000, "MarketConsult", "employee", "analytics"));

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

		// begin s1
		DataStream<FinancialTransaction> s1 = env.addSource(new FinancialTransactionFixedSource(1000, 0, workload))
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<FinancialTransaction>() {

					@Override
					public long extractAscendingTimestamp(FinancialTransaction element) {
						return element.getEventTime();
					}
				});

		// s1 privacy conf
		app.getStreamByID("s1").setConcreteStream(s1);
		//
		// end s1

		// begin s2
		DataStream<TransactionsCount> s2 = s1.keyBy("dataSubject").timeWindow(Time.seconds(1))
				.apply(new TransactionCounter());
		app.getStreamByID("s2").setConcreteStream(s2);
		// end s2

		// begin s3
		DataStream<TotalExpense> s3 = s1.keyBy("dataSubject").timeWindow(Time.seconds(1))
				.apply(new TotalExpenseCalculator());
		app.getStreamByID("s3").setConcreteStream(s3);
		// end s3

		///////////////////////////////////////////////////////////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		// s2 privacy conf

		ApplicationDataStream app_s2 = app.getStreamByID("s2");

		ProtectedStream<TransactionsCount> s2_p = new ProtectedStream<TransactionsCount>(false, "", -1, 1, false, 0, "/home/utente/eclipse-workspace/library/results");
		s2_p.setStreamToProtect((DataStream<TransactionsCount>) app_s2.getConcreteStream());

		for (VCP vcp : app.getVCPs(app_s2.getId())) {
			s2_p.addVCP(app_s2, (VCP) vcp, app);
		}

		for (DSEP dsep : app.getDSEPs(app_s2.getId())) {
			s2_p.addDSEP(app_s2, (DSEP) dsep, app);
		}

		DataStream<TransactionsCount> s2_f = s2_p.finalize(env, contextStream);
		s2_f.writeAsText("/home/utente/eclipse-workspace/library/results/s2.txt", WriteMode.OVERWRITE)
				.setParallelism(1);
		// end s2 privacy conf

		// s3 privacy conf
		ApplicationDataStream app_s3 = app.getStreamByID("s3");

		ProtectedStream<TotalExpense> s3_p = new ProtectedStream<TotalExpense>(false, "", -1, 1, false, 0, "/home/utente/eclipse-workspace/library/results");
		s3_p.setStreamToProtect((DataStream<TotalExpense>) app_s3.getConcreteStream());

		s3_p.addGeneralizationFunction("totalAmount", new Integer(1), new GeneralizationFunction());

		for (VCP vcp : app.getVCPs(app_s3.getId())) {
			s3_p.addVCP(app_s3, (VCP) vcp, app);
		}

		for (DSEP dsep : app.getDSEPs(app_s3.getId())) {
			s3_p.addDSEP(app_s3, (DSEP) dsep, app);
		}
		DataStream<TotalExpense> s3_f = s3_p.finalize(env, contextStream);
		s3_f.writeAsText("/home/utente/eclipse-workspace/library/results/s3.txt", WriteMode.OVERWRITE)
				.setParallelism(1);
		// end s3 privacy conf

		// begin s4
		DataStream<TopConsumersCount> s4 = s3_f.timeWindowAll(Time.seconds(6)).apply(new TopConsumersCounter());
		app.getStreamByID("s4").setConcreteStream(s4);
		s4.writeAsText("/home/utente/eclipse-workspace/library/results/s4.txt", WriteMode.OVERWRITE).setParallelism(1);
		// end s4
		
		s1.writeAsText("/home/utente/eclipse-workspace/library/results/s1.txt", WriteMode.OVERWRITE).setParallelism(1);

		contextStream.writeAsText("/home/utente/eclipse-workspace/library/results/ctx.txt", WriteMode.OVERWRITE).setParallelism(1);
		
		try (PrintWriter out = new PrintWriter("/home/utente/eclipse-workspace/library/results/plan.json")) {
			out.println(env.getExecutionPlan());
			out.close();
		}
		
		
		env.execute();
		
		List<String> streamNames = app.getApplicationStreamsNames();
		streamNames.add("ctx");
						
		StreamMerger.genericMerge("/home/utente/eclipse-workspace/library/results",  streamNames, "/home/utente/eclipse-workspace/library/trace-checking/financial/financial.log");

	}

}
