package example1.application;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.GeneralizationVector;
import it.deib.polimi.diaprivacy.model.PastCondition;
import it.deib.polimi.diaprivacy.model.PrivacyContext;
import it.deib.polimi.diaprivacy.model.RelationalOperator;
import it.deib.polimi.diaprivacy.model.VariableType;
import library.GeneralizationFunction;
import library.GeneralizationLevel;
import library.SubjectSpecificConditionChecker;
import library.PrivacyContextParser;
import library.MaskedStream;
import library.MaskBuilder;
import example1.datatypes.SubjectSpecific;
import example1.utils.PrivacyContextFixedSource;
import example1.utils.PrivacyContextRandomSource;
import example1.utils.StreamMerger;
import example1.utils.SubjectDerivedRandomSource;
import example1.utils.SubjectSpecificFixedSource;
import example1.utils.SubjectSpecificRandomSource;
import example1.datatypes.SubjectDerived;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class example1 {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties prop = new Properties();
		prop.load(new FileInputStream("config.properties"));
		//prop.load(new FileInputStream(args[0]));

		String timestampServerIp = prop.getProperty("timestampServerIp");
		String pathToResultFolder = prop.getProperty("pathToResultFolder");
		Integer timestampServerPort = Integer.parseInt(prop.getProperty("timestampServerPort"));
		Boolean privacyOn = Boolean.parseBoolean(prop.getProperty("privacyOn"));
		Integer topologyParallelism = Integer.parseInt(prop.getProperty("topologyParallelism"));
		Integer minIntervalBetweenTransactions = Integer.parseInt(prop.getProperty("minIntervalBetweenTransactions"));
		Integer maxIntervalBetweenTransactions = Integer.parseInt(prop.getProperty("maxIntervalBetweenTransactions"));
		Integer nTuples = Integer.parseInt(prop.getProperty("nTuples"));
		Integer nDataSubject = Integer.parseInt(prop.getProperty("nDataSubject"));
		Integer minIntervalBetweenContextSwitch = Integer.parseInt(prop.getProperty("minIntervalBetweenContextSwitch"));
		Integer maxIntervalBetweenContextSwitch = Integer.parseInt(prop.getProperty("maxIntervalBetweenContextSwitch"));
		Integer nContextSwitch = Integer.parseInt(prop.getProperty("nContextSwitch"));
		Boolean isNanoSeconds = Boolean.parseBoolean(prop.getProperty("isNanoSeconds"));
		Integer bufferTimeout = Integer.parseInt(prop.getProperty("bufferTimeout"));
		Integer nPastCond = Integer.parseInt(prop.getProperty("nPastCond"));
		Integer warmUpTuples = Integer.parseInt(prop.getProperty("warmUpTuples"));
		Integer coolDownTuples = Integer.parseInt(prop.getProperty("coolDownTuples"));
		Boolean simulateRealisticScenario = Boolean.parseBoolean(prop.getProperty("simulateRealisticScenario"));
		Integer minDelay = Integer.parseInt(prop.getProperty("minDelay"));
		Integer maxDelay = Integer.parseInt(prop.getProperty("maxDelay"));
		Integer allowedLateness = Integer.parseInt(prop.getProperty("allowedLateness"));

		Integer initialDelay = 0;
		Boolean notNanoSeconds = false;
		Integer minContent = 50;
		Integer maxContent = 200;
		Integer sleepBeforeFinish = 5000;
		Boolean observed = true;
		Boolean notObserved = false;

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(Integer.parseInt(prop.getProperty("topologyParallelism")));
		env.setBufferTimeout(Integer.parseInt(prop.getProperty("bufferTimeout")));

		DataStream<SubjectSpecific> s1 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions, maxIntervalBetweenTransactions, notNanoSeconds, minContent, maxContent,
				nTuples, sleepBeforeFinish, observed, "s1", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, false, 2000L, minDelay, maxDelay));

		DataStream<SubjectSpecific> s2 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions, maxIntervalBetweenTransactions, notNanoSeconds, minContent,
				maxContent, nTuples, sleepBeforeFinish, false, "s2", warmUpTuples, coolDownTuples,
				timestampServerIp, timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));

		DataStream<SubjectSpecific> s3 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions, maxIntervalBetweenTransactions, notNanoSeconds, minContent,
				maxContent, nTuples, sleepBeforeFinish, false, "s3", warmUpTuples, coolDownTuples,
				timestampServerIp, timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));

		DataStream<SubjectDerived> s4 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions, maxIntervalBetweenTransactions, notNanoSeconds, minContent,
				maxContent, nTuples, sleepBeforeFinish, false, "s4", warmUpTuples, coolDownTuples,
				timestampServerIp, timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		
		DataStream<SubjectDerived> s5 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions, maxIntervalBetweenTransactions, notNanoSeconds, minContent,
				maxContent, nTuples, sleepBeforeFinish, false, "s5", warmUpTuples, coolDownTuples,
				timestampServerIp, timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		
/*		List<Tuple2<SubjectSpecific, Long>> workload1 = new ArrayList<Tuple2<SubjectSpecific, Long>>();
		workload1.add(
				new Tuple2<SubjectSpecific, Long>(new SubjectSpecific("ds1", 120, "t1", observed, 1000L, "s1"), 400L));
		workload1.add(
				new Tuple2<SubjectSpecific, Long>(new SubjectSpecific("ds1", 131, "t2", observed, 1400L, "s1"), 400L));
		workload1.add(
				new Tuple2<SubjectSpecific, Long>(new SubjectSpecific("ds1", 137, "t3", observed, 1800L, "s1"), 0L));

		List<Tuple2<SubjectSpecific, Long>> workload2 = new ArrayList<Tuple2<SubjectSpecific, Long>>();

		if (simulateRealisticScenario) {
			// introducing lateness (not unordering)
			workload2.add(new Tuple2<SubjectSpecific, Long>(
					new SubjectSpecific("ds1", 125, "t1", !observed, 1300L, "s2"), 1100L));
			workload2.add(new Tuple2<SubjectSpecific, Long>(
					new SubjectSpecific("ds1", 138, "t2", !observed, 1600L, "s2"), 0L));
			// introducing lateness (not unordering)
		} else {
			workload2.add(new Tuple2<SubjectSpecific, Long>(
					new SubjectSpecific("ds1", 125, "t1", !observed, 1200L, "s2"), 400L));
			workload2.add(new Tuple2<SubjectSpecific, Long>(
					new SubjectSpecific("ds1", 138, "t2", !observed, 1600L, "s2"), 0L));
		}
		
		// realistic scenario for s3
		List<Tuple2<SubjectSpecific, Long>> workload3 = new ArrayList<Tuple2<SubjectSpecific, Long>>();
		workload3.add(
				new Tuple2<SubjectSpecific, Long>(new SubjectSpecific("ds1", 120, "t1", !observed, 1700L, "s3"), 450L));
		workload3.add(
				new Tuple2<SubjectSpecific, Long>(new SubjectSpecific("ds1", 140, "t1", !observed, 1750L, "s3"), 0L));

		DataStream<SubjectSpecific> s1 = env.addSource(new SubjectSpecificFixedSource(200, sleepBeforeFinish, observed,
				"s1", timestampServerIp, timestampServerPort, false, workload1));
		DataStream<SubjectSpecific> s2 = env.addSource(new SubjectSpecificFixedSource(500, sleepBeforeFinish, !observed,
				"s2", timestampServerIp, timestampServerPort, simulateRealisticScenario, workload2));
		DataStream<SubjectSpecific> s3 = env.addSource(new SubjectSpecificFixedSource(900, sleepBeforeFinish, !observed,
				"s3", timestampServerIp, timestampServerPort, false, workload3));
*/
		
		s1.writeAsText(pathToResultFolder + "/s1.txt").setParallelism(1);

		s2.writeAsText(pathToResultFolder + "/s2.txt").setParallelism(1);

		s3.writeAsText(pathToResultFolder + "/s3.txt").setParallelism(1);

		s4.writeAsText(pathToResultFolder + "/s4.txt").setParallelism(1);
		
		s5.writeAsText(pathToResultFolder + "/s5.txt").setParallelism(1);

		if (privacyOn) {
			MaskedStream<SubjectSpecific> s1_p = new MaskedStream<SubjectSpecific>(
					timestampServerIp, timestampServerPort, topologyParallelism, simulateRealisticScenario,
					allowedLateness);
			s1_p.setStreamToProtect(s1);

			s1_p.addGeneralizationFunction("content", new Integer(1), new GeneralizationFunction());


			PastCondition sspc = new PastCondition("content", VariableType.INTEGER, RelationalOperator.GREATER, new Integer(130), 0, 300);
			Map<DataStream<?>, PastCondition> subjectSpecificPastConds = new HashMap<DataStream<?>, PastCondition>();
			subjectSpecificPastConds.put(s2, sspc);
			
			PastCondition gpc = new PastCondition("content", VariableType.INTEGER, RelationalOperator.GREATER, new Integer(130), 0, 300);
			Map<DataStream<?>, PastCondition> genericPastConds = new HashMap<DataStream<?>, PastCondition>();
			genericPastConds.put(s4, gpc);

			List<ContextualCondition> protectedStreamConds = new ArrayList<ContextualCondition>();
			protectedStreamConds.add(new ContextualCondition("content", VariableType.INTEGER, RelationalOperator.GREATER,
					new Integer(130)));
			
			ContextualCondition sssc = new ContextualCondition("content", VariableType.INTEGER, RelationalOperator.GREATER, new Integer(130));
			Map<DataStream<?>, ContextualCondition> subjectSpecificStaticConds = new HashMap<DataStream<?>, ContextualCondition>();
			subjectSpecificStaticConds.put(s3, sssc);
			
			ContextualCondition gsc = new ContextualCondition("content", VariableType.INTEGER, RelationalOperator.GREATER, new Integer(131));
			Map<DataStream<?>, ContextualCondition> genericStaticConds = new HashMap<DataStream<?>, ContextualCondition>();
			genericStaticConds.put(s5, gsc);

			GeneralizationVector gv = new GeneralizationVector();
			gv.setVariableGenLevel("content", 1);
			
			s1_p.addPolicy("ds1", subjectSpecificPastConds, genericPastConds, subjectSpecificStaticConds, genericStaticConds, new PrivacyContext("u1", "employee", "marketing"), protectedStreamConds, gv);

			DataStreamSource<PrivacyContext> contextStream = env.addSource(new PrivacyContextFixedSource(0, 2000));
			s1_p.finalize(env, contextStream).writeAsText(pathToResultFolder + "/s1_p.txt").setParallelism(1);

			contextStream.writeAsText(pathToResultFolder + "/ctx.txt").setParallelism(1);
		}

		try (PrintWriter out = new PrintWriter(pathToResultFolder + "/plan.json")) {
			out.println(env.getExecutionPlan());
			out.close();
		}

		JobExecutionResult result = env.execute();

		Socket s = new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort);

		PrintStream socketWriter = new PrintStream(s.getOutputStream());
		socketWriter.println("jobEnd");
		s.close();

		try (PrintWriter out = new PrintWriter(pathToResultFolder + "/throughput.txt")) {
			out.println((double) nTuples / result.getNetRuntime(TimeUnit.MILLISECONDS));
			out.close();
		}

		System.out.println("######### THROUGHPUT: "
				+ (double) nTuples / result.getNetRuntime(TimeUnit.MILLISECONDS) + " ######### \n");

		System.out.println("Merging output for trace checking.");
		StreamMerger.merge(pathToResultFolder);
		// StreamMerger.merge(new File(pathToResultFolder + "/s1.txt"), new
		// File(pathToResultFolder + "/s2.txt"),
		// new File(pathToResultFolder + "/merged.txt"));

	}
}
