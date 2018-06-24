package library;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.PastCondition;
import it.deib.polimi.diaprivacy.model.PrivacyContext;

public class EvictedStream<T, S> {

	private Integer topologyParallelism;
	private Boolean simulateRealisticScenario;
	private Integer allowedLateness;
	private Integer nPastConditionCheckers;


	private DataStream<Tuple3<String, S, List<Boolean>>> intermediateStream;

	private SubjectEvictor<S> dataSubjectEvictor;

	private AllWindowFunction<S, T, TimeWindow> operator;

	private Time timeWindow;
	
	private SubjectSpecificConditionChecker<S, ?> lastPcc;

	private DataStream<?> lastOtherDataSubjectSpecificStream;

	public EvictedStream() {

	}

	public EvictedStream(String timestampServerIp, Integer timestampServerPort,
			Integer topologyParallelism, Boolean simulateRealisticScenario, Integer allowedLateness) {
		this.simulateRealisticScenario = simulateRealisticScenario;
		this.allowedLateness = allowedLateness;
		this.topologyParallelism = topologyParallelism;
		this.dataSubjectEvictor = new SubjectEvictor<S>(timestampServerIp, timestampServerPort);
		this.nPastConditionCheckers = 0;
		this.lastPcc = null;
		this.lastOtherDataSubjectSpecificStream = null;
	}

	public void setStreamToProtect(DataStream<S> inputStream, AllWindowFunction<S, T, TimeWindow> operator,
			Time timeWindow) {

		this.operator = operator;
		this.timeWindow = timeWindow;

		this.intermediateStream = inputStream.map(new MapFunction<S, Tuple3<String, S, List<Boolean>>>() {

			private static final long serialVersionUID = -8436658880715586251L;

			@Override
			public Tuple3<String, S, List<Boolean>> map(S t) throws Exception {
				Field f = t.getClass().getDeclaredField("dataSubject");
				f.setAccessible(true);
				return new Tuple3<String, S, List<Boolean>>((String) f.get(t), t, new ArrayList<Boolean>());
			}
		}).setParallelism(topologyParallelism);

	}

	public void setContextualPattern(String dataSubject, PrivacyContext pc, List<ContextualCondition> conds) {
		this.dataSubjectEvictor.setPrivacyContextPreference(dataSubject, pc);
		for (ContextualCondition c : conds) {
			this.dataSubjectEvictor.setProtectedStreamPreference(dataSubject, c);
		}
	}

	@SuppressWarnings("unchecked")
	public <U> void addSubjectSpecificStreamPastConditions(DataStream<U> otherDataSubjectSpecificStream,
			Map<String, PastCondition> conds) {
		
		if (lastPcc != null && lastOtherDataSubjectSpecificStream != null) {
			DataStream<U> tmp = (DataStream<U>) this.lastOtherDataSubjectSpecificStream;
			DataStream<Tuple2<String, U>> intermediateOtherStream = tmp
					.map(new MapFunction<U, Tuple2<String, U>>() {

						private static final long serialVersionUID = 7214612742034395543L;

						@Override
						public Tuple2<String, U> map(U t) throws Exception {
							Field f = t.getClass().getDeclaredField("dataSubject");
							f.setAccessible(true);
							return new Tuple2<String, U>((String) f.get(t), t);
						}
					}).setParallelism(topologyParallelism);

			this.intermediateStream = this.intermediateStream.connect(intermediateOtherStream).keyBy(0, 0)
					.flatMap((SubjectSpecificConditionChecker<S, U>) this.lastPcc).setParallelism(topologyParallelism);
		}

		SubjectSpecificConditionChecker<S, U> pastPolicyChecker = new SubjectSpecificConditionChecker<S, U>(this.allowedLateness, this.simulateRealisticScenario, nPastConditionCheckers == 0 ? true : false);
		this.nPastConditionCheckers = nPastConditionCheckers + 1;
		for (String ds : conds.keySet()) {
			pastPolicyChecker.addPastCondition(ds, conds.get(ds));
		}
		
		this.lastPcc = pastPolicyChecker;
		this.lastOtherDataSubjectSpecificStream = otherDataSubjectSpecificStream;
		
	}

	public DataStream<T> finalize(StreamExecutionEnvironment env, DataStream<PrivacyContext> contextStream) {
		return this.intermediateStream.connect(contextStream.broadcast())
				// .keyBy(_._1, _.serviceUser)
				.flatMap(this.dataSubjectEvictor).setParallelism(1).timeWindowAll(this.timeWindow).apply(this.operator)
				.setParallelism(1);
	}

}
