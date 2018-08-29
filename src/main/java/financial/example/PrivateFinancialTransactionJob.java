package financial.example;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.yaml.snakeyaml.Yaml;

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

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(0);

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
		
		/// privacy init
		Yaml yaml = new Yaml();

		String content = FileUtils.readFileToString(
				new File("/home/utente/eclipse-workspace/library/src/main/java/financial/example/privacy-config.yml"), "UTF-8");

		ApplicationPrivacy app = yaml.loadAs(content, ApplicationPrivacy.class);
		
		DataStreamSource<PrivacyContext> contextStream = env.addSource(new PrivacyContextFixedSource(0, 2000, "MarketConsult", "employee", "analytics"));

		app.getStreamByID("s1").setConcreteStream(s1);
		app.getStreamByID("s2").setConcreteStream(s2);
		app.getStreamByID("s3").setConcreteStream(s3);
		app.getStreamByID("s4").setConcreteStream(s4);
		// end

		s2.writeAsText("/home/utente/eclipse-workspace/library/results/s2.txt", WriteMode.OVERWRITE);

		
		//protecting s3 before writing to sink
		ApplicationDataStream app_s3 = app.getStreamByID("s3");

		ProtectedStream<TotalExpense> s3_p = new ProtectedStream<TotalExpense>(false, "",
				-1, 1, false, 0);
		s3_p.setStreamToProtect((DataStream<TotalExpense>) app_s3.getConcreteStream());

		s3_p.addGeneralizationFunction("totalAmount", new Integer(1), new GeneralizationFunction());

		for (VCP vcp : app.getVCPs(app_s3.getId())) {
			s3_p.addVCP(app_s3, (VCP) vcp, app);
		}

		for (DSEP dsep : app.getDSEPs(app_s3.getId())) {
			s3_p.addDSEP(app_s3, (DSEP) dsep, app);
		}
		s3_p.finalize(env, contextStream).writeAsText("/home/utente/eclipse-workspace/library/results/s3.txt", WriteMode.OVERWRITE)
				.setParallelism(1);
		// end
		
		
		//protecting s4 before writing to sink
		ApplicationDataStream app_s4 = app.getStreamByID("s4");

		ProtectedStream<TopConsumersCount> s4_p = new ProtectedStream<TopConsumersCount>(false, "",
				-1, 1, false, 0);
		s4_p.setStreamToProtect((DataStream<TopConsumersCount>) app_s4.getConcreteStream());

		for (DSEP dsep : app.getDSEPs(app_s4.getId())) {
			s4_p.addDSEP(app_s4, (DSEP) dsep, app);
		}
		s4_p.finalize(env, contextStream).writeAsText("/home/utente/eclipse-workspace/library/results/s4.txt", WriteMode.OVERWRITE)
				.setParallelism(1);
		// end
		
		
		env.execute();

	}

}
