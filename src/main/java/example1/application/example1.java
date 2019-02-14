package example1.application;

import it.deib.polimi.diaprivacy.library.GeneralizationFunction;
import it.deib.polimi.diaprivacy.library.PrivacyContextFixedSource;
import it.deib.polimi.diaprivacy.library.ProtectedStream;
import it.deib.polimi.diaprivacy.model.ApplicationDataStream;
import it.deib.polimi.diaprivacy.model.ApplicationPrivacy;
import it.deib.polimi.diaprivacy.model.DSEP;
import it.deib.polimi.diaprivacy.model.PrivacyContext;
import it.deib.polimi.diaprivacy.model.PrivacyPolicy;
import it.deib.polimi.diaprivacy.model.VCP;
import example1.datatypes.SubjectSpecific;
import example1.utils.StreamMerger;
import example1.utils.SubjectDerivedRandomSource;
import example1.utils.SubjectSpecificRandomSource;
import example1.datatypes.SubjectDerived;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.yaml.snakeyaml.Yaml;

public class example1 {


	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties prop = new Properties();
		//prop.load(new FileInputStream("config.properties"));
		prop.load(new FileInputStream(args[0]));

		String timestampServerIp = prop.getProperty("timestampServerIp");
		String pathToResultFolder = prop.getProperty("pathToResultFolder");
		Boolean monitoringActive = Boolean.parseBoolean(prop.getProperty("monitoringActive"));
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
		
		Integer trafficScaleFactor = 1;

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(Integer.parseInt(prop.getProperty("topologyParallelism")));
		env.setBufferTimeout(Integer.parseInt(prop.getProperty("bufferTimeout")));

		/// privacy init
		Yaml yaml = new Yaml();

		//String content = FileUtils.readFileToString(
			//	new File("/home/utente/eclipse-workspace/policySyntesizer/src/example1.yml"), "UTF-8");
		
		String content = FileUtils.readFileToString(
				new File(args[1]), "UTF-8");

		ApplicationPrivacy app = yaml.loadAs(content, ApplicationPrivacy.class);

		DataStreamSource<PrivacyContext> contextStream = env.addSource(new PrivacyContextFixedSource(0, 2000, "u1", "employee", "marketing"));

		// APP
		DataStream<SubjectSpecific> s2 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s2", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, false, 2000L, minDelay, maxDelay));
		app.getStreamByID("s2").setConcreteStream(s2);

		DataStream<SubjectSpecific> s3 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s3", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s3").setConcreteStream(s3);


		DataStream<SubjectDerived> s4 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s4", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s4").setConcreteStream(s4);


		DataStream<SubjectDerived> s5 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s5", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s5").setConcreteStream(s5);
		
		DataStream<SubjectSpecific> s7 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s7", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s7").setConcreteStream(s7);
		
		DataStream<SubjectSpecific> s8 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s8", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s8").setConcreteStream(s8);
		
		DataStream<SubjectSpecific> s9 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s9", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s9").setConcreteStream(s9);
		
		DataStream<SubjectDerived> s10 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s10", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s10").setConcreteStream(s10);
		
		DataStream<SubjectDerived> s11 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s11", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s11").setConcreteStream(s11);
		
		DataStream<SubjectDerived> s12 = env.addSource(new SubjectDerivedRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions*trafficScaleFactor, maxIntervalBetweenTransactions*trafficScaleFactor, notNanoSeconds, minContent, maxContent,
				nTuples/trafficScaleFactor, sleepBeforeFinish, !observed, "s12", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, simulateRealisticScenario, 2000L, minDelay, maxDelay));
		app.getStreamByID("s12").setConcreteStream(s12);

	    // generating sink for s1 (START)
		// for each stream that goes into a sink and that is subject specific, generate the stream
		// and set all the policies
/*		DataStream<SubjectSpecific> s1 = env.addSource(new SubjectSpecificRandomSource(initialDelay, nDataSubject,
				minIntervalBetweenTransactions, maxIntervalBetweenTransactions, notNanoSeconds, minContent, maxContent,
				nTuples, sleepBeforeFinish, observed, "s1", warmUpTuples, coolDownTuples, timestampServerIp,
				timestampServerPort, false, 2000L, minDelay, maxDelay));
		
		ApplicationDataStream app_s1 = app.getStreamByID("s1");
		app_s1.setConcreteStream(s1);

		ProtectedStream<SubjectSpecific> s1_p = new ProtectedStream<SubjectSpecific>(monitoringActive, timestampServerIp,
				timestampServerPort, topologyParallelism, simulateRealisticScenario, allowedLateness, pathToResultFolder);
		s1_p.setStreamToProtect((DataStream<SubjectSpecific>) app_s1.getConcreteStream());

		s1_p.addGeneralizationFunction("content", new Integer(1), new GeneralizationFunction());

		for (VCP vcp : app.getVCPs(app_s1.getId())) {
			s1_p.addVCP(app_s1, vcp, app);
		}

		for (DSEP dsep : app.getDSEPs(app_s1.getId())) {
			s1_p.addDSEP(app_s1, dsep, app);
		}
		//s1_p.finalize(env, contextStream);
		
		
		// necessary for trace-checking
		s1_p.finalize(env, contextStream).writeAsText(pathToResultFolder + "/" + app_s1.getId() + "_p.txt", WriteMode.OVERWRITE)
				.setParallelism(1);*/
		// necessary for trace-checking
	    // generating sink for s1 (END)

		// generating sink for s6 (START)
		// for each stream that goes into a sink and that is generic, than generate the stream
		// and set all the policies
	
		
		ApplicationDataStream app_s6 = app.getStreamByID("s6");
		ProtectedStream<SubjectSpecific> s6_evicted_input = new ProtectedStream<SubjectSpecific>(monitoringActive, timestampServerIp,
				timestampServerPort, topologyParallelism, simulateRealisticScenario, allowedLateness, pathToResultFolder);
		s6_evicted_input.setStreamToProtect(s2);

		for (DSEP p : app.getDSEPs(app_s6.getId())) {
			if (!app_s6.getIsSubjectSpecific()) {
				s6_evicted_input.addDSEP(app_s6, p, app);
			}
		}

		DataStream<SubjectSpecific> s2_evicted = s6_evicted_input.finalize(env, contextStream);
		
		DataStream<Integer> s6 = s2_evicted.map(x -> x.getContent())
				.timeWindowAll(Time.milliseconds(500)).sum(0).setParallelism(1);

		app_s6.setConcreteStream(s6);
		
		
		// generating sink for s6 (END)
		
		// necessary for trace checking
		s6.writeAsText(pathToResultFolder + "/" + app_s6.getId() + "_p.txt", WriteMode.OVERWRITE).setParallelism(1);

		
		//s1.writeAsText(pathToResultFolder + "/s1.txt", WriteMode.OVERWRITE).setParallelism(1);

		s2_evicted.writeAsText(pathToResultFolder + "/s2.txt", WriteMode.OVERWRITE).setParallelism(1);

		s3.writeAsText(pathToResultFolder + "/s3.txt", WriteMode.OVERWRITE).setParallelism(1);

		s4.writeAsText(pathToResultFolder + "/s4.txt", WriteMode.OVERWRITE).setParallelism(1);

		s5.writeAsText(pathToResultFolder + "/s5.txt", WriteMode.OVERWRITE).setParallelism(1);

		DataStream<Integer> s6_org = s2.map(x -> x.getContent()).timeWindowAll(Time.milliseconds(500)).sum(0);
		s6_org.writeAsText(pathToResultFolder + "/s6.txt", WriteMode.OVERWRITE).setParallelism(1);
		
		
		contextStream.writeAsText(pathToResultFolder + "/ctx.txt", WriteMode.OVERWRITE).setParallelism(1);
		// necessary for trace checking
		
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

		System.out.println("######### THROUGHPUT: " + (double) nTuples / result.getNetRuntime(TimeUnit.MILLISECONDS)
				+ " ######### \n");

		
		System.out.println("Merging output for trace checking.");
		StreamMerger.mergeDsep(pathToResultFolder);
		
		// StreamMerger.merge(new File(pathToResultFolder + "/s1.txt"), new
		// File(pathToResultFolder + "/s2.txt"),
		// new File(pathToResultFolder + "/merged.txt"));

	}
}
