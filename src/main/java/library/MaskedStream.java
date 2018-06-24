package library;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.GeneralizationVector;
import it.deib.polimi.diaprivacy.model.PastCondition;
import it.deib.polimi.diaprivacy.model.PrivacyContext;
import scala.collection.mutable.HashSet;

public class MaskedStream<T> {

	private Integer topologyParallelism;
	private Boolean simulateRealisticScenario;
	private Integer allowedLateness;
	private Integer nPastConditionCheckers;

	private DataStream<Tuple3<String, T, List<Boolean>>> intermediateStream;

	private MaskBuilder<T> viewBuilder;

	private ConditionChecker<T, ?> lastPcc;

	private boolean isLastPccSubjectSpecific;

	private DataStream<?> lastOtherDataSubjectSpecificStream;

	private List<SubjectSpecificConditionChecker<T, ?>> addedSubjectSpecificPccs;

	private List<GenericConditionChecker<T, ?>> addedGenericPccs;

	private HashSet<String> dsWithAtLeastOnePastCondition;

	public MaskedStream() {

	}

	public MaskedStream(String timestampServerIp, Integer timestampServerPort, Integer topologyParallelism,
			Boolean simulateRealisticScenario, Integer allowedLateness) {
		this.simulateRealisticScenario = simulateRealisticScenario;
		this.allowedLateness = allowedLateness;
		this.topologyParallelism = topologyParallelism;
		this.viewBuilder = new MaskBuilder<T>(timestampServerIp, timestampServerPort);
		this.nPastConditionCheckers = 0;
		this.lastPcc = null;
		this.lastOtherDataSubjectSpecificStream = null;
		this.addedSubjectSpecificPccs = new ArrayList<SubjectSpecificConditionChecker<T, ?>>();
		this.addedGenericPccs = new ArrayList<GenericConditionChecker<T, ?>>();
		this.dsWithAtLeastOnePastCondition = new HashSet<String>();
	}

	@SuppressWarnings("unchecked")
	public void addPolicy(String ds, Map<DataStream<?>, PastCondition> subjectSpecificPastConditions,
			Map<DataStream<?>, PastCondition> genericPastConditions,
			Map<DataStream<?>, ContextualCondition> subjectSpecificStaticConditions,
			Map<DataStream<?>, ContextualCondition> genericStaticConditions, PrivacyContext privacyContext,
			List<ContextualCondition> protectedStreamConds, GeneralizationVector gv) {

		boolean pccAlreadyExists;
		for (DataStream<?> pastConditionedStream : subjectSpecificPastConditions.keySet()) {
			pccAlreadyExists = false;
			for (SubjectSpecificConditionChecker<T, ?> pcc : addedSubjectSpecificPccs) {
				if (pcc.getAssociatedStream() == pastConditionedStream.getId()) {
					pcc.addPastCondition(ds, subjectSpecificPastConditions.get(pastConditionedStream));
					pccAlreadyExists = true;
				}
			}

			if (!pccAlreadyExists) {
				SubjectSpecificConditionChecker<T, ?> pcc = this.addSubjectSpecificStreamCondition(
						pastConditionedStream, ds, subjectSpecificPastConditions.get(pastConditionedStream));
				pcc.setAssociatedStream(pastConditionedStream.getId());
				this.addedSubjectSpecificPccs.add(pcc);

			}
		}

		for (DataStream<?> pastConditionedStream : genericPastConditions.keySet()) {
			pccAlreadyExists = false;
			for (GenericConditionChecker<T, ?> pcc : addedGenericPccs) {
				if (pcc.getAssociatedStream() == pastConditionedStream.getId()) {
					pcc.addPastCondition(ds, genericPastConditions.get(pastConditionedStream));
					pccAlreadyExists = true;
				}
			}

			if (!pccAlreadyExists) {
				GenericConditionChecker<T, ?> pcc = this.addGenericStreamCondition(pastConditionedStream, ds,
						genericPastConditions.get(pastConditionedStream));
				pcc.setAssociatedStream(pastConditionedStream.getId());
				this.addedGenericPccs.add(pcc);

			}
		}

		for (DataStream<?> conditionedStream : subjectSpecificStaticConditions.keySet()) {
			pccAlreadyExists = false;
			for (SubjectSpecificConditionChecker<T, ?> pcc : addedSubjectSpecificPccs) {
				if (pcc.getAssociatedStream() == conditionedStream.getId()) {
					pcc.addStaticCondition(ds, subjectSpecificStaticConditions.get(conditionedStream));
					pccAlreadyExists = true;
				}
			}

			if (!pccAlreadyExists) {
				SubjectSpecificConditionChecker<T, ?> pcc = this.addSubjectSpecificStreamCondition(conditionedStream,
						ds, subjectSpecificStaticConditions.get(conditionedStream));
				pcc.setAssociatedStream(conditionedStream.getId());
				this.addedSubjectSpecificPccs.add(pcc);

			}
		}
		
		for (DataStream<?> conditionedStream : genericStaticConditions.keySet()) {
			pccAlreadyExists = false;
			for (GenericConditionChecker<T, ?> pcc : addedGenericPccs) {
				if (pcc.getAssociatedStream() == conditionedStream.getId()) {
					pcc.addStaticCondition(ds, genericStaticConditions.get(conditionedStream));
					pccAlreadyExists = true;
				}
			}

			if (!pccAlreadyExists) {
				GenericConditionChecker<T, ?> pcc = this.addGenericStreamCondition(conditionedStream, ds,
						genericStaticConditions.get(conditionedStream));
				pcc.setAssociatedStream(conditionedStream.getId());
				this.addedGenericPccs.add(pcc);

			}
		}

		this.setContextualPattern(ds, privacyContext, protectedStreamConds);
		this.addGeneralizationVector(ds, gv);
		if ((subjectSpecificPastConditions != null && !subjectSpecificPastConditions.isEmpty())
				|| (genericPastConditions != null && !genericPastConditions.isEmpty())
				|| (subjectSpecificStaticConditions != null && !subjectSpecificStaticConditions.isEmpty())
				|| (genericStaticConditions != null && !genericStaticConditions.isEmpty())) {
			this.dsWithAtLeastOnePastCondition.add(ds);
		}
	}

	public void setStreamToProtect(DataStream<T> toProtect) {
		this.intermediateStream = toProtect.map(new MapFunction<T, Tuple3<String, T, List<Boolean>>>() {

			private static final long serialVersionUID = -8436658880715586251L;

			@Override
			public Tuple3<String, T, List<Boolean>> map(T t) throws Exception {
				Field f = t.getClass().getDeclaredField("dataSubject");
				f.setAccessible(true);
				return new Tuple3<String, T, List<Boolean>>((String) f.get(t), t, new ArrayList<Boolean>());
			}
		}).setParallelism(topologyParallelism);

	}

	public void addGeneralizationFunction(String attribute, Integer level, GeneralizationFunction f) {
		this.viewBuilder.setGeneralizationLevel(attribute, level, f);
	}

	private void addGeneralizationVector(String dataSubject, GeneralizationVector gv) {
		this.viewBuilder.setGeneralizationVector(dataSubject, gv);
	}

	private void setContextualPattern(String dataSubject, PrivacyContext pc, List<ContextualCondition> conds) {
		this.viewBuilder.setPrivacyContextPreference(dataSubject, pc);
		for (ContextualCondition c : conds) {
			this.viewBuilder.setProtectedStreamPreference(dataSubject, c);
		}
	}

	@SuppressWarnings("unchecked")
	private <S> SubjectSpecificConditionChecker addSubjectSpecificStreamCondition(
			DataStream<S> otherDataSubjectSpecificStream, String ds, ContextualCondition cond) {
		
		if (lastPcc != null && lastOtherDataSubjectSpecificStream != null) {
			if (isLastPccSubjectSpecific) {
				DataStream<S> tmp = (DataStream<S>) this.lastOtherDataSubjectSpecificStream;
				DataStream<Tuple2<String, S>> intermediateOtherStream = tmp
						.map(new MapFunction<S, Tuple2<String, S>>() {

							private static final long serialVersionUID = 7214612742034395543L;

							@Override
							public Tuple2<String, S> map(S t) throws Exception {
								Field f = t.getClass().getDeclaredField("dataSubject");
								f.setAccessible(true);
								return new Tuple2<String, S>((String) f.get(t), t);
							}
						}).setParallelism(topologyParallelism);

				this.intermediateStream = this.intermediateStream.connect(intermediateOtherStream).keyBy(0, 0)
						.flatMap((SubjectSpecificConditionChecker<T, S>) this.lastPcc)
						.setParallelism(topologyParallelism);
			} else {
				DataStream<S> tmp = (DataStream<S>) this.lastOtherDataSubjectSpecificStream;

				this.intermediateStream = this.intermediateStream.connect(tmp.broadcast())
						.flatMap((GenericConditionChecker<T, S>) this.lastPcc).setParallelism(topologyParallelism);
			}
		}

		SubjectSpecificConditionChecker<T, S> pastPolicyChecker = new SubjectSpecificConditionChecker<T, S>(
				this.allowedLateness, this.simulateRealisticScenario, nPastConditionCheckers == 0 ? true : false);
		this.nPastConditionCheckers = nPastConditionCheckers + 1;

		if (cond instanceof PastCondition) {
			pastPolicyChecker.addPastCondition(ds, (PastCondition) cond);
		} else {
			pastPolicyChecker.addStaticCondition(ds, cond);
		}

		this.lastPcc = pastPolicyChecker;
		this.isLastPccSubjectSpecific = true;
		this.lastOtherDataSubjectSpecificStream = otherDataSubjectSpecificStream;

		return pastPolicyChecker;
	}

	@SuppressWarnings("unchecked")
	private <S> GenericConditionChecker addGenericStreamCondition(DataStream<S> otherGenericStream, String ds,
			ContextualCondition cond) {
		
		if (lastPcc != null && lastOtherDataSubjectSpecificStream != null) {
			if (isLastPccSubjectSpecific) {
				DataStream<S> tmp = (DataStream<S>) this.lastOtherDataSubjectSpecificStream;
				DataStream<Tuple2<String, S>> intermediateOtherStream = tmp
						.map(new MapFunction<S, Tuple2<String, S>>() {

							private static final long serialVersionUID = 7214612742034395543L;

							@Override
							public Tuple2<String, S> map(S t) throws Exception {
								Field f = t.getClass().getDeclaredField("dataSubject");
								f.setAccessible(true);
								return new Tuple2<String, S>((String) f.get(t), t);
							}
						}).setParallelism(topologyParallelism);

				this.intermediateStream = this.intermediateStream.connect(intermediateOtherStream).keyBy(0, 0)
						.flatMap((SubjectSpecificConditionChecker<T, S>) this.lastPcc)
						.setParallelism(topologyParallelism);
			} else {
				DataStream<S> tmp = (DataStream<S>) this.lastOtherDataSubjectSpecificStream;

				this.intermediateStream = this.intermediateStream.connect(tmp.broadcast())
						.flatMap((GenericConditionChecker<T, S>) this.lastPcc).setParallelism(topologyParallelism);
			}
		}

		GenericConditionChecker<T, S> pastPolicyChecker = new GenericConditionChecker<T, S>(this.allowedLateness,
				this.simulateRealisticScenario, nPastConditionCheckers == 0 ? true : false);
		this.nPastConditionCheckers = nPastConditionCheckers + 1;

		if (cond instanceof PastCondition) {
			pastPolicyChecker.addPastCondition(ds, (PastCondition) cond);
		} else {
			pastPolicyChecker.addStaticCondition(ds, cond);
		}

		this.lastPcc = pastPolicyChecker;
		this.isLastPccSubjectSpecific = false;
		this.lastOtherDataSubjectSpecificStream = otherGenericStream;

		return pastPolicyChecker;
	}

	@SuppressWarnings("unchecked")
	public <S> DataStream<T> finalize(StreamExecutionEnvironment env, DataStream<PrivacyContext> contextStream) {

		if (lastPcc != null && lastOtherDataSubjectSpecificStream != null) {
			this.lastPcc.setIsLast(true);
			if (isLastPccSubjectSpecific) {
				DataStream<S> tmp = (DataStream<S>) this.lastOtherDataSubjectSpecificStream;
				DataStream<Tuple2<String, S>> intermediateOtherStream = tmp
						.map(new MapFunction<S, Tuple2<String, S>>() {

							private static final long serialVersionUID = 7214612742034395543L;

							@Override
							public Tuple2<String, S> map(S t) throws Exception {
								Field f = t.getClass().getDeclaredField("dataSubject");
								f.setAccessible(true);
								return new Tuple2<String, S>((String) f.get(t), t);
							}
						}).setParallelism(topologyParallelism);

				this.intermediateStream = this.intermediateStream.connect(intermediateOtherStream).keyBy(0, 0)
						.flatMap((SubjectSpecificConditionChecker<T, S>) this.lastPcc)
						.setParallelism(topologyParallelism);
			} else {
				DataStream<S> tmp = (DataStream<S>) this.lastOtherDataSubjectSpecificStream;

				this.intermediateStream = this.intermediateStream.connect(tmp.broadcast())
						.flatMap((GenericConditionChecker<T, S>) this.lastPcc).setParallelism(topologyParallelism);
			}

		}

		for (SubjectSpecificConditionChecker<T, ?> pcc : this.addedSubjectSpecificPccs) {
			pcc.setDsWithAtLeastOneCondition(this.dsWithAtLeastOnePastCondition);
		}

		for (GenericConditionChecker<T, ?> pcc : this.addedGenericPccs) {
			pcc.setDsWithAtLeastOneCondition(this.dsWithAtLeastOnePastCondition);
		}

		return this.intermediateStream.connect(contextStream.broadcast())
				// .keyBy(_._1, _.serviceUser)
				.flatMap(this.viewBuilder).setParallelism(topologyParallelism);
	}

}
