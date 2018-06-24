package library;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.PastCondition;
import scala.collection.mutable.HashSet;

import org.apache.flink.api.java.tuple.Tuple3;

public class GenericConditionChecker<T, S>
		extends RichCoFlatMapFunction<Tuple3<String, T, List<Boolean>>, S, Tuple3<String, T, List<Boolean>>>
		implements ConditionChecker {

	private static final long serialVersionUID = 1L;

	// there should be a list of conditions for each user
	private Map<String, PastCondition> pastConditionPerDataSubject;

	private Map<String, ContextualCondition> conditionPerDataSubject;

	private S lastValue;

	private List<S> retainedOtherStreamWindow;

	private ConcurrentHashMap<T, Tuple3<String, List<S>, S>> tupleMetadata;

	private Map<T, Boolean> resultPerTuple;

	private Map<T, List<Boolean>> otherResults;

	private Boolean isFirst;

	private Boolean isLast = false;

	private int associatedStream;

	private HashSet<String> dsWithAtLeastOneCondition;

	public Boolean getIsLast() {
		return isLast;
	}

	public void setIsLast(Boolean isLast) {
		this.isLast = isLast;
	}

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(50);

	private Integer alowedLateness;
	private Boolean processingInEventTime;

	public GenericConditionChecker(Integer allowedLateness, Boolean processingInEventTime, Boolean isFirst) {
		this.alowedLateness = allowedLateness;
		this.processingInEventTime = processingInEventTime;
		this.isFirst = isFirst;
		this.tupleMetadata = new ConcurrentHashMap<T, Tuple3<String, List<S>, S>>();
		this.pastConditionPerDataSubject = new HashMap<String, PastCondition>();
		this.conditionPerDataSubject = new HashMap<String, ContextualCondition>();
		this.retainedOtherStreamWindow = new ArrayList<S>();
		this.resultPerTuple = new HashMap<T, Boolean>();
		this.otherResults = new HashMap<T, List<Boolean>>();
	}

	@Override
	public void flatMap1(Tuple3<String, T, List<Boolean>> value, Collector<Tuple3<String, T, List<Boolean>>> out)
			throws Exception {

		Field e = value.f1.getClass().getDeclaredField("eventTime");
		e.setAccessible(true);
		System.out.println("Received tuple from the main stream: " + value.f1);

		if (pastConditionPerDataSubject.containsKey(value.f0)) {
			System.out.println("There exists a past condition for the data subject " + value.f0);
			PastCondition cond = this.pastConditionPerDataSubject.get(value.f0);
			if (this.processingInEventTime && this.alowedLateness > 0) {
				if (!this.tupleMetadata.containsKey(value.f1)) {
					System.out.println("First time that this tuple is received. Init phase.");
					// if not contained and list is empty we are in the initialization phase (firsst
					// time that I see this tuple)

					// create a new entry inwindowPerTuple and populate the list with the current
					// content
					List<S> initWindow = new ArrayList<S>();
					for (S t : retainedOtherStreamWindow) {
						Field eg = t.getClass().getDeclaredField("eventTime");
						eg.setAccessible(true);
						if ((Long) eg.get(t) > (Long) e.get(value.f1) - cond.timeWindowMilliseconds()) {
							initWindow.add(t);
						}
					}

					// flushing from the retainedWindow all the tuples older than the max window in
					// the past specified by the managed past conditions
					if (lastValue != null) {
						PastCondition pc = this.pastConditionPerDataSubject.get(value.f0);
						java.util.Iterator<S> iter = this.retainedOtherStreamWindow.iterator();
						while (iter.hasNext()) {
							S rt = iter.next();
							Field eg = rt.getClass().getDeclaredField("eventTime");
							eg.setAccessible(true);
							if ((Long) eg.get(rt) < (Long) eg.get(lastValue) - pc.getUpperTemporalBound()) {
								iter.remove();
							}
						}
					}
					//

					System.out.println("Initial window from stream " + this.getAssociatedStream());

					for (S i : initWindow) {
						System.out.println(i);
					}

					if (this.conditionPerDataSubject.containsKey(value.f0)) {
						System.out.println("Data subject " + value.f0 + " has a static condition on the stream "
								+ this.associatedStream
								+ ". Setting the value to be used currently for evaluating it.");
						if (lastValue != null) {
							System.out.println("There exists already a value from stream " + this.associatedStream
									+ " about " + value.f0);
							////////////////////////////// !!!!!!!!!!!!!!!!!!
							if ((Long) e.get(value.f1) > (Long) e.get(lastValue)) {
								System.out.println(
										"The existing value is older than the tuple from the  main stream just received. Setting this as value to be considered for static the condition.");
								this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, lastValue));
							} else {
								System.out.println(
										"The existing value is newer than the tuple from the main stream just received. Setting no value for evaluating the static condition. ###########");
								this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));
							}
						} else {
							System.out.println("No esisting tuple from the stream " + this.associatedStream
									+ " about data subject " + value.f0
									+ ". Setting no value for evaluating the static condition.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));
						}
					} else {
						System.out.println("Data subject " + value.f0 + " has no static condition on the stream "
								+ this.associatedStream + ". Setting no value for evaluating static condition.");
						this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));
					}

					this.resultPerTuple.put(value.f1, null);

					// start timer
					this.eventTimeFlatMap1(value, out);
					// forward the tuple downstream
					if (!isLast) {
						out.collect(value);
					}
				} else if (this.tupleMetadata.containsKey(value.f1)) {
					// if contained and list is not empty we are second (the computation) phase
					System.out.println(
							"The just received tuple from the main stream was previously scheduled for processing.");
					// if result have been not computed yet
					if (this.resultPerTuple.get(value.f1) == null) {
						System.out.println(
								"The tuple still have to be processed processed. Saving the other results and waiting for the timer to expire.");
						this.otherResults.put(value.f1, value.f2);
					} else {
						System.out.println("The tuple has been already processed and the result is available: "
								+ this.resultPerTuple.get(value.f1));
						List<Boolean> updated = value.f2;
						updated.add(this.resultPerTuple.get(value.f1));

						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							System.out.println("There exist also a static condition for data subject " + value.f0
									+ " on the stream " + this.associatedStream
									+ ". Computing the truth value and adding to the other values before emitting the tuple from the main stream.");
							boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
							updated.add(result);
							System.out.println("Result is: " + result);

						}

						Tuple3<String, T, List<Boolean>> output = new Tuple3<String, T, List<Boolean>>();
						output.setFields(value.f0, value.f1, updated);
						out.collect(output);
						this.tupleMetadata.remove(value.f1);
						this.resultPerTuple.remove(value.f1);
					}

				}

			} else {
				this.initializeWindow(value.f0, value.f1, cond);
				this.checkTupleCondition(value, out);
				this.tupleMetadata.remove(value.f1);

				if (this.conditionPerDataSubject.containsKey(value.f0)) {
					boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
					value.f2.add(result);
				}

				out.collect(value);
			}
		} else if (!pastConditionPerDataSubject.containsKey(value.f0)
				&& this.dsWithAtLeastOneCondition.contains(value.f0)) {
			System.out.println("The data subject " + value.f0 + " does not have a past condition over the stream "
					+ this.associatedStream + " but there exists a past condition from him along the chain.");
			if (this.processingInEventTime && this.alowedLateness > 0) {
				if (!isLast && !isFirst) {
					System.out.println("This PCC is in the middle of the chain.");
					if (this.tupleMetadata.contains(value.f1)) {
						System.out.println("It is the second time that this tuple is received.");
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							System.out.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Computing and adding the value before emitting.");
							System.out.println(this.conditionPerDataSubject.get(value.f0));
							boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
							value.f2.add(result);
							System.out.println("Result is: " + result);
						}
						out.collect(value);
					} else {
						System.out.println(
								"It is the first time that this tuple is received. Waiting for the second time.");

						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							System.out.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream);
							///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
							if (lastValue != null) {
								Field t = this.lastValue.getClass().getDeclaredField("eventTime");
								t.setAccessible(true);
								if ((Long) t.get(this.lastValue) < (Long) e.get(value.f1)) {
									System.out.println("There is a value about " + value.f0 + " from the stream "
											+ this.associatedStream
											+ ". Setting this as the last value associated to the waiting tuple.");
									this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, lastValue));
								} else {
									System.out.println("There is a value about " + value.f0 + " from the stream "
											+ this.associatedStream + ", but it is too new."
											+ ". Setting no value associated to the waiting tuple.");
									this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
								}
							} else {
								System.out.println("There is still no value about " + value.f0 + " from the stream "
										+ this.associatedStream
										+ ". Setting no last value associated to the waiting tuple.");
								this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
							}
						} else {
							System.out.println("There exists no static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Setting no last value associated to the waiting tuple.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
						}

						out.collect(value);
					}
				} else if (isLast && !isFirst) {
					System.out.println("This PCC is the last of the chain.");
					if (this.tupleMetadata.containsKey(value.f1)) {
						System.out.println("It is the second time that this tuple is received.");
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							System.out.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Computing and adding the value before emitting.");

							boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
							value.f2.add(result);
							System.out.println("Result is: " + result);
						}
						out.collect(value);
					} else {
						System.out.println(
								"It is the first time that this tuple is received. Waiting for the second time.");
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							System.out.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream);
							///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
							if (lastValue != null) {
								Field t = this.lastValue.getClass().getDeclaredField("eventTime");
								t.setAccessible(true);
								if ((Long) t.get(this.lastValue) < (Long) e.get(value.f1)) {
									System.out.println("There is a value about " + value.f0 + " from the stream "
											+ this.associatedStream
											+ ". Setting this as the last value associated to the waiting tuple.");
									this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, lastValue));
								} else {
									System.out.println("There is a value about " + value.f0 + " from the stream "
											+ this.associatedStream + ", but it is too new."
											+ ". Setting no value associated to the waiting tuple.");
									this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
								}

							} else {
								System.out.println("There is still no value about " + value.f0 + " from the stream "
										+ this.associatedStream
										+ ". Setting no last value associated to the waiting tuple.");
								this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
							}
						} else {
							System.out.println("There exists no static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Setting no last value associated to the waiting tuple.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
						}
					}
				} else if (isLast && isFirst) {
					System.out.println("This PCC is the first of the chain.");
					if (this.processingInEventTime && this.alowedLateness > 0) {
						System.out.println("Scheduling processing of the received tuple and forwarding.");
						this.eventTimeFlatMap1(value, out);
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							System.out.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream);
							///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
							if (this.lastValue != null) {
								System.out.println("There is a value from the stream "
										+ this.associatedStream
										+ ". Setting this as the last value associated to the waiting tuple.");
								this.tupleMetadata.put(value.f1,
										new Tuple3<>(value.f0, null, this.lastValue));
								System.out.println("Updated value: " + this.tupleMetadata.get(value.f1).f2);
							} else {
								System.out.println("There is still no value about " + value.f0 + " from the stream "
										+ this.associatedStream
										+ ". Setting no last value associated to the waiting tuple.");
								this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
							}
						} else {
							System.out.println("There exists no static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Setting no last value associated to the waiting tuple.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
						}					} else {
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							value.f2.add(this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0)));
						}
						out.collect(value);
					}			
				} else if (!isLast && isFirst) {
					System.out.println("This PCC is the first of the chain.");
					System.out.println("Scheduling processing of the received tuple and forwarding.");
					this.eventTimeFlatMap1(value, out);
					out.collect(value);

				}
			} else {
				if (this.conditionPerDataSubject.containsKey(value.f0)) {
					boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
					value.f2.add(result);
				}

				out.collect(value);
			}
		} else if (!pastConditionPerDataSubject.containsKey(value.f0)
				&& !this.dsWithAtLeastOneCondition.contains(value.f0)) {
			out.collect(value);
		}
	}

	private boolean checkCondition(T t, ContextualCondition cond)
			throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {

		S lastTuple;

		if (this.tupleMetadata.get(t) != null) {
			System.out.println("The tuple is currently waiting (event time case)");
			lastTuple = this.tupleMetadata.get(t).f2;

		} else {
			System.out.println("The tuple is no waiting (processing time case)");
			lastTuple = this.lastValue;
		}

		System.out.println(lastTuple + "ààààààààà#########");
		Field a = lastTuple.getClass().getDeclaredField(cond.getVariable());
		a.setAccessible(true);

		if (lastTuple != null) {
			System.out.println("Found tuple from the stream " + this.associatedStream
					+ " associated to the tuple being processed from the main stream. Using that to check stati condition.");
			switch (cond.getOperator()) {
			case EQUAL: {
				return a.get(lastTuple).equals(cond.getValue());
			}
			case NOT_EQUAL: {
				return !a.get(lastTuple).equals(cond.getValue());
			}
			case GREATER: {
				return (Integer) a.get(lastTuple) > (Integer) cond.getValue();
			}
			case GREATER_OR_EQUAL: {
				return (Integer) a.get(lastTuple) >= (Integer) cond.getValue();
			}
			case LESS: {
				return (Integer) a.get(lastTuple) < (Integer) cond.getValue();
			}
			case LESS_OR_EQUAL: {
				return (Integer) a.get(lastTuple) <= (Integer) cond.getValue();
			}
			default:
				return false;
			}
		} else {
			if (this.processingInEventTime) {
				System.out.println("Processing in event time and found no tuple from stream " + this.associatedStream
						+ " associated to the main stream tuple being processed. It should be that when the mainstream tuple arrived there"
						+ "was no available tuple from the other stream e no tuple arrived in the waiting time.");
			} else {
				System.out.println("Processing in processing time and found no tuple from stream "
						+ this.associatedStream
						+ " associated to the main stream tuple being processed. It should be that no tuple from the associated stream has been received yet.");
			}
			return false;
		}

	}

	private void initializeWindow(String ds, T tuple, PastCondition cond)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		Field e = tuple.getClass().getDeclaredField("eventTime");
		e.setAccessible(true);

		// create a new entry inwindowPerTuple and populate the list with the current
		// content
		List<S> initWindow = new ArrayList<S>();
		for (S t : retainedOtherStreamWindow) {
			if ((Long) e.get(t) > (Long) e.get(tuple) - cond.timeWindowMilliseconds()) {
				initWindow.add(t);
			}
		}
		if (lastValue != null) {
			this.tupleMetadata.put(tuple, new Tuple3<>(ds, initWindow, lastValue));
		} else {
			this.tupleMetadata.put(tuple, new Tuple3<>(ds, initWindow, null));
		}

		// flushing from the retainedWindow all the tuples older than the max window in
		// the past specified by the managed past conditions
		PastCondition pc = this.pastConditionPerDataSubject.get(ds);
		java.util.Iterator<S> iter = this.retainedOtherStreamWindow.iterator();
		while (iter.hasNext()) {
			S rt = iter.next();
			if ((Long) e.get(rt) < (Long) e.get(lastValue) - pc.getUpperTemporalBound()) {
				iter.remove();
			}
		}
		//
	}

	private void checkTupleCondition(Tuple3<String, T, List<Boolean>> value,
			Collector<Tuple3<String, T, List<Boolean>>> out) throws Exception {

		System.out.println("Checking conditions for tuple " + value.f1);

		if (pastConditionPerDataSubject.containsKey(value.f0)) {

			System.out.println("There exists a past condition specified by data subject " + value.f0 + " on stream "
					+ this.associatedStream);

			PastCondition userPastEventPolicy = this.pastConditionPerDataSubject.get(value.f0);

			List<S> window = this.tupleMetadata.get(value.f1).f1;

			switch (userPastEventPolicy.getOperator()) {
			case EQUAL: {
				this.resultPerTuple.put(value.f1, this.existsEqual(window, userPastEventPolicy));
				break;
			}
			case NOT_EQUAL: {
				this.resultPerTuple.put(value.f1, !this.existsEqual(window, userPastEventPolicy));
				break;
			}
			case GREATER: {
				this.resultPerTuple.put(value.f1, this.existsGreater(window, userPastEventPolicy));
				break;
			}
			case GREATER_OR_EQUAL: {
				this.resultPerTuple.put(value.f1, this.existsGreaterOrEqual(window, userPastEventPolicy));
				break;
			}
			case LESS: {
				this.resultPerTuple.put(value.f1, this.existsLess(window, userPastEventPolicy));
				break;

			}
			case LESS_OR_EQUAL: {
				this.resultPerTuple.put(value.f1, this.existsLessOrEqual(window, userPastEventPolicy));
				break;
			}
			}
		}

		System.out.println("The result of the past condition checking is: " + this.resultPerTuple.get(value.f1));

		if (this.isFirst) {
			System.out.println(
					"This is the first PCC on the chian, so it is going to trigger the releasing of the tuple.");
			if (this.pastConditionPerDataSubject.containsKey(value.f0)) {
				System.out.println(
						"Adding to the tuple the result for the past condition specified by data subject " + value.f0);
				value.f2.add(this.resultPerTuple.get(value.f1));
				this.resultPerTuple.remove(value.f1);
				if (this.processingInEventTime) {
					this.tupleMetadata.remove(value.f1);
				}
			}

			if (this.conditionPerDataSubject.containsKey(value.f0)) {
				System.out.println("Adding to the tuple the result for the static condition specified by data subject "
						+ value.f0);
				value.f2.add(this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0)));
			}
			out.collect(value);
		}

		if (this.otherResults.containsKey(value.f1)) {
			System.out.println(
					"This is a PCC for which the timer of the current tuple was already expired and was waiting for the tuple being received for the second time.");
			System.out.println("Adding to the tuple all the results that were stored for it.");
			value.f2.add(this.resultPerTuple.get(value.f1));
			for (Boolean result : this.otherResults.get(value.f1)) {
				value.f2.add(result);
			}
			this.tupleMetadata.remove(value.f1);
			this.resultPerTuple.remove(value.f1);

			if (this.conditionPerDataSubject.containsKey(value.f0)) {
				System.out.println("Adding to the tuple the result for the static condition specified by data subject "
						+ value.f0);
				value.f2.add(this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0)));
			}

			out.collect(value);
		}
	}

	private void eventTimeFlatMap1(Tuple3<String, T, List<Boolean>> value,
			Collector<Tuple3<String, T, List<Boolean>>> out) throws Exception {
		scheduler.schedule(new Runnable() {
			@Override
			public void run() {
				try {
					checkTupleCondition(value, out);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, alowedLateness, TimeUnit.MILLISECONDS);
	}

	@Override
	public void flatMap2(S value, Collector<Tuple3<String, T, List<Boolean>>> out) throws Exception {

		System.out.println("Received new tuple on stream " + this.associatedStream + ": " + value);

		Field tg = value.getClass().getDeclaredField("eventTime");
		tg.setAccessible(true);

		this.retainedOtherStreamWindow.add(value);

		System.out.println("Updating the windows associated to each waiting tuple which");

		for (T tuple : tupleMetadata.keySet()) {
			List<S> updatedList = tupleMetadata.get(tuple).f1;

			Field ds = tuple.getClass().getDeclaredField("dataSubject");
			ds.setAccessible(true);

			Field ts = tuple.getClass().getDeclaredField("eventTime");
			ts.setAccessible(true);

			if (this.pastConditionPerDataSubject.containsKey((String) ds.get(tuple))) {
				PastCondition userPastEventPolicy = this.pastConditionPerDataSubject.get((String) ds.get(tuple));

				if ((Long) tg.get(value) >= (Long) ts.get(tuple) - userPastEventPolicy.timeWindowMilliseconds()
						&& (Long) tg.get(value) <= (Long) ts.get(tuple)) {
					System.out.println(
							"The just received tuple has a timestamp which is older than the waiting tuple but within the window specified by the past condition. Updating the window associated to the waiting tuple.");
					updatedList.add(value);
				} else {
					System.out.println(
							"The just received tuple has a timestamp which is either newer than the waiting tuple or that do not belong to the window specified by the past condition. No update.");
				}
			}
		}

		System.out.println("Updating the last value associated to each waiting tuple");
		for (T tuple : this.tupleMetadata.keySet()) {

			Field ts = tuple.getClass().getDeclaredField("eventTime");
			ts.setAccessible(true);

			if (this.tupleMetadata.get(tuple).f2 != null) {
				if ((Long) tg.get(value) > (Long) tg.get(this.tupleMetadata.get(tuple).f2)
						&& (Long) tg.get(value) <= (Long) ts.get(tuple)) {
					System.out.println(
							"The just received tuple has a timestamp which is older than the waiting tuple but newer than the currently associated last value. Updating last value associated to the waiting tuple.");
					this.tupleMetadata.put(tuple,
							new Tuple3<>(this.tupleMetadata.get(tuple).f0, this.tupleMetadata.get(tuple).f1, value));
					System.out.println("Updated last value: " + this.tupleMetadata.get(tuple).f2);
				} else {
					System.out.println(
							"The just received tuple has a timestamp which is either newer than the waiting tuple or older than the currently associated last value. No update.");
				}
			} else {
				System.out.println("There is no previous last value from stream " + this.associatedStream
						+ " associated to the waiting tuple");
				if ((Long) tg.get(value) <= (Long) ts.get(tuple)) {
					System.out.println(
							"The just received tuple has a timestamp which is older than the waiting tuple. Updating last value associated to the waiting tuple.");

					this.tupleMetadata.put(tuple,
							new Tuple3<>(this.tupleMetadata.get(tuple).f0, this.tupleMetadata.get(tuple).f1, value));
				} else {
					System.out.println(
							"The just received tuple has a timestamp which is newer than the waiting tuple. No update.");
				}
			}
		}

		System.out.println(
				"Updating the most recent value which has been globally seen for stream " + this.associatedStream);
		if (this.lastValue != null) {
			System.out.println("There is already a previous value.");
			if ((Long) tg.get(value) > (Long) tg.get(lastValue)) {
				System.out.println("The previous value is older than the one just received. Updating.");
				this.lastValue = value;
			} else {
				System.out.println("The previous value is newer than the one just received. Non updating.");
			}
		} else {
			System.out.println("There is no previous value. Updating");
			this.lastValue = value;
		}
		System.out.println("Updated value: " + this.lastValue);
	}

	public void addPastCondition(String dataSubject, PastCondition cond) {
		this.pastConditionPerDataSubject.put(dataSubject, cond);
	}

	public void addStaticCondition(String dataSubject, ContextualCondition cond) {
		this.conditionPerDataSubject.put(dataSubject, cond);
	}

	private Boolean existsEqual(List<S> toControl, PastCondition cond) {

		Field var;

		for (S t : toControl) {
			try {
				var = t.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if (var.get(t).equals(cond.getValue())) {
					return true;
				}
			} catch (NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return false;
	}

	private Boolean existsGreater(List<S> toControl, PastCondition cond) {
		Field var;

		for (S t : toControl) {
			try {
				var = t.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);

				if ((Integer) var.get(t) > (Integer) cond.getValue()) {
					return true;
				}
			} catch (NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return false;
	}

	private Boolean existsGreaterOrEqual(List<S> toControl, PastCondition cond) {
		Field var;

		for (S t : toControl) {
			try {
				var = t.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if ((Integer) var.get(t) >= (Integer) cond.getValue()) {
					return true;
				}
			} catch (NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	private Boolean existsLess(List<S> toControl, PastCondition cond) {
		Field var;

		for (S t : toControl) {
			try {
				var = t.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if ((Integer) var.get(t) < (Integer) cond.getValue()) {
					return true;
				}
			} catch (NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	private Boolean existsLessOrEqual(List<S> toControl, PastCondition cond) {
		Field var;

		for (S t : toControl) {
			try {
				var = t.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if ((Integer) var.get(t) <= (Integer) cond.getValue()) {
					return true;
				}
			} catch (NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	private class EventTimeComparator implements Comparator<S> {

		@Override
		public int compare(S a, S b) {
			try {
				Field comparable;
				comparable = a.getClass().getDeclaredField("eventTime");
				comparable.setAccessible(true);
				return (Long) comparable.get(a) < (Long) comparable.get(b) ? -1
						: ((Long) comparable.get(a)).equals((Long) comparable.get(b)) ? 0 : 1;
			} catch (NoSuchFieldException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return -1;
		}
	}

	public int getAssociatedStream() {
		return associatedStream;
	}

	public void setAssociatedStream(int associatedStream) {
		this.associatedStream = associatedStream;
	}

	public void setDsWithAtLeastOneCondition(HashSet<String> dsWithAtLeastOnePolicy) {
		this.dsWithAtLeastOneCondition = dsWithAtLeastOnePolicy;
	}

	@Override
	public void close() throws Exception {
		scheduler.shutdown();
		super.close();
	}

}
