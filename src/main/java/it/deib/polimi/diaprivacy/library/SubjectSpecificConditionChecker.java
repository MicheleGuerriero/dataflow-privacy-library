package it.deib.polimi.diaprivacy.library;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.PastCondition;
import scala.collection.mutable.HashSet;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;

public class SubjectSpecificConditionChecker<T, S> extends
		RichCoFlatMapFunction<Tuple3<String, T, List<Boolean>>, Tuple2<String, S>, Tuple3<String, T, List<Boolean>>>
		implements ConditionChecker {

	private static final long serialVersionUID = 1L;

	// there should be a list of conditions for each user
	private Map<String, PastCondition> pastConditionPerDataSubject;

	private Map<String, ContextualCondition> conditionPerDataSubject;

	private Map<String, S> lastValuePerSubject;

	private List<Tuple2<String, S>> retainedOtherStreamWindow;

	private List<Tuple2<String, S>> retainedOtherStreamLastValue;

	private ConcurrentHashMap<T, Tuple3<String, List<Tuple2<String, S>>, S>> tupleMetadata;

	private Map<T, Boolean> resultPerTuple;

	private Map<T, List<Boolean>> otherResults;

	private Boolean isFirst;

	private Boolean isLast = false;

	private int associatedStream;

	private PrintWriter writer;

	private HashSet<String> dsWithAtLeastOneCondition;
	
	private String logDir;

	public Boolean getIsLast() {
		return isLast;
	}

	public void setIsLast(Boolean isLast) {
		this.isLast = isLast;
	}

	private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1200);

	private Integer alowedLateness;
	private Boolean processingInEventTime;

	public SubjectSpecificConditionChecker(String logDir, Integer allowedLateness, Boolean processingInEventTime, Boolean isFirst) {
		this.logDir = logDir;
		this.alowedLateness = allowedLateness;
		this.processingInEventTime = processingInEventTime;
		this.isFirst = isFirst;
		this.tupleMetadata = new ConcurrentHashMap<T, Tuple3<String, List<Tuple2<String, S>>, S>>();
		this.pastConditionPerDataSubject = new HashMap<String, PastCondition>();
		this.lastValuePerSubject = new HashMap<String, S>();
		this.conditionPerDataSubject = new HashMap<String, ContextualCondition>();
		this.retainedOtherStreamWindow = new ArrayList<Tuple2<String, S>>();
		this.retainedOtherStreamLastValue = new ArrayList<Tuple2<String, S>>();
		this.resultPerTuple = new HashMap<T, Boolean>();
		this.otherResults = new HashMap<T, List<Boolean>>();
	}

	@Override
	public void flatMap1(Tuple3<String, T, List<Boolean>> value, Collector<Tuple3<String, T, List<Boolean>>> out)
			throws Exception {

		Field e1 = value.f1.getClass().getDeclaredField("eventTime");
		e1.setAccessible(true);
		
		Field e2 = null;
				
		writer.println("Received tuple from the main stream: " + value.f1);

		//for each incoming tuple t_i:=(ts,id,ds,cnt)
		// if there exists a past condition for on s_2 specified by ds for enabling the protection of s_1
		if (pastConditionPerDataSubject.containsKey(value.f0)) {
			writer.println("There exists a past condition for the data subject " + value.f0);
			PastCondition cond = this.pastConditionPerDataSubject.get(value.f0);
			// if processing in event time
			if (this.processingInEventTime && this.alowedLateness > 0) {
				// if it is the first time that t_i is received (window init phase)
				if (!this.tupleMetadata.containsKey(value.f1)) {
					writer.println("First time that this tuple is received. Init phase.");

					// initialize w_i
					List<Tuple2<String, S>> initWindow = new ArrayList<Tuple2<String, S>>();
					for (Tuple2<String, S> t : retainedOtherStreamWindow) {
						if (e2 == null) {
							e2 = t.f1.getClass().getDeclaredField("eventTime");
							e2.setAccessible(true);
						}
						if (t.f0.equals(value.f0)) {
							if ((Long) e2.get(t.f1) > (Long) e1.get(value.f1) - cond.timeWindowMilliseconds()) {
								initWindow.add(t);
							}
						}
					}

					// flushing from the retainedWindow all the tuples older than the max window in
					// the past specified by the managed past conditions
					if (this.lastValuePerSubject.get(value.f0) != null) {
						PastCondition pc = this.pastConditionPerDataSubject.get(value.f0);
						java.util.Iterator<Tuple2<String, S>> iter = this.retainedOtherStreamWindow.iterator();
						while (iter.hasNext()) {
							Tuple2<String, S> rt = iter.next();
							if (rt.f0.equals(value.f0)) {
								if (e2 == null) {
									e2 = rt.f1.getClass().getDeclaredField("eventTime");
									e2.setAccessible(true);
								}
								if ((Long) e2.get(rt.f1) < (Long) e2.get(this.lastValuePerSubject.get(value.f0))
										- pc.getUpperTemporalBound()) {
									iter.remove();
								}
							}
						}
					}
					//

					writer.println("Initial window from stream " + this.getAssociatedStream());

					for (Tuple2<String, S> i : initWindow) {
						writer.println(i.f1.toString());
					}

					if (this.conditionPerDataSubject.containsKey(value.f0)) {
						writer.println("Data subject " + value.f0 + " has a static condition on the stream "
								+ this.associatedStream
								+ ". Setting the value to be used currently for evaluating it.");
						if (this.lastValuePerSubject.get(value.f0) != null) {
							writer.println("There exists already a value from stream " + this.associatedStream
									+ " about " + value.f0);
							if (e2 == null) {
								e2 = this.lastValuePerSubject.get(value.f0).getClass().getDeclaredField("eventTime");
								e2.setAccessible(true);
							}
							////////////////////////////// !!!!!!!!!!!!!!!!!!
							if ((Long) e1.get(value.f1) > (Long) e2.get(this.lastValuePerSubject.get(value.f0))) {
								writer.println(
										"The existing value is older than the tuple from the  main stream just received. Setting this as value to be considered for static the condition.");
								this.tupleMetadata.put(value.f1,
										new Tuple3<>(value.f0, initWindow, this.lastValuePerSubject.get(value.f0)));
							} else {
								writer.println(
										"The existing value is newer than the tuple from the main stream just received. Setting no value for evaluating the static condition. ###########");
								///////////////////////////////// HERE THE VALUE SHOULD BE PICKED FROM THE
								///////////////////////////////// RETAINED LIST
								///////////////////////////////// WHICH SHOULD ALSO BE UPDATED
								if (this.retainedOtherStreamLastValue.isEmpty()) {
									this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));
								} else {
									S last = null;
									Field ts = this.retainedOtherStreamLastValue.get(0).f1.getClass()
											.getDeclaredField("eventTime");
									ts.setAccessible(true);
									for (Tuple2<String, S> t : this.retainedOtherStreamLastValue) {
										if (t.f0.equals(value.f0)) {
											if (last == null) {
												if ((Long) ts.get(t.f1) <= (Long) e1.get(value.f1)) {
													last = t.f1;
												}
											} else {
												if ((Long) ts.get(t.f1) <= (Long) e1.get(value.f1)
														&& (Long) ts.get(t.f1) > (Long) ts.get(last)) {
													last = t.f1;
												}
											}
										}
									}

									if (last != null) {
										this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, last));
										Iterator<Tuple2<String, S>> iter = this.retainedOtherStreamLastValue.iterator();
										while (iter.hasNext()) {
											Tuple2<String, S> t = iter.next();
											if (t.f0.equals(value.f0)) {
												if ((Long) ts.get(t) < (Long) ts.get(last)) {
													iter.remove();
												}
											}
										}
									} else {
										this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));
									}
									/////////////////////////////////
									/////////////////////////////////
								}
							}
						} else {
							writer.println("No esisting tuple from the stream " + this.associatedStream
									+ " about data subject " + value.f0
									+ ". Setting no value for evaluating the static condition.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));

						}
					} else {
						
						writer.println("Data subject " + value.f0 + " has no static condition on the stream "
								+ this.associatedStream + ". Setting no value for evaluating static condition.");
						this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, initWindow, null));
					}

					// store the mapping between t_i and w_i
					this.resultPerTuple.put(value.f1, null);

					// start timer
					this.eventTimeFlatMap1(value, out);
					// forward the tuple downstream
					if (!isLast) {
						out.collect(value);
					}
				} else if (this.tupleMetadata.containsKey(value.f1)) {
					// if contained and list is not empty we are second (the computation) phase
					writer.println(
							"The just received tuple from the main stream was previously scheduled for processing.");
					// if result have been not computed yet
					if (this.resultPerTuple.get(value.f1) == null) {
						writer.println(
								"The tuple still have to be processed processed. Saving the other results and waiting for the timer to expire.");
						// stores the upstream truth values
						this.otherResults.put(value.f1, value.f2);
					} else {
						writer.println("The tuple has been already processed and the result is available: "
								+ this.resultPerTuple.get(value.f1));
						List<Boolean> updated = value.f2;
						updated.add(this.resultPerTuple.get(value.f1));

						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							writer.println("There exist also a static condition for data subject " + value.f0
									+ " on the stream " + this.associatedStream
									+ ". Computing the truth value and adding to the other values before emitting the tuple from the main stream.");
							boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
							updated.add(result);
							writer.println("Result is: " + result);

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
			writer.println("The data subject " + value.f0 + " does not have a past condition over the stream "
					+ this.associatedStream + " but there exists a condition fom him along the chain.");
			if (this.processingInEventTime && this.alowedLateness > 0) {
				if (!isLast && !isFirst) {
					writer.println("This PCC is in the middle of the chain.");
					if (this.tupleMetadata.containsKey(value.f1)) {
						writer.println("It is the second time that this tuple is received.");
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							writer.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Computing and adding the value before emitting.");
							boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
							value.f2.add(result);
							writer.println("Result is: " + result);
						}
						out.collect(value);
					} else {
						writer.println(
								"It is the first time that this tuple is received. Waiting for the second time.");

						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							writer.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream);
							///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
							this.setLastValue(value);
						} else {
							writer.println("There exists no static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Setting no last value associated to the waiting tuple.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
						}

						out.collect(value);
					}
				} else if (isLast && !isFirst) {
					writer.println("This PCC is the last of the chain.");
					if (this.tupleMetadata.containsKey(value.f1)) {
						writer.println("It is the second time that this tuple is received.");
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							writer.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Computing and adding the value before emitting.");

							boolean result = this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0));
							value.f2.add(result);
							writer.println("Result is: " + result);
						}
						out.collect(value);
					} else {
						writer.println(
								"It is the first time that this tuple is received. Waiting for the second time.");
						if (this.conditionPerDataSubject.containsKey(value.f0)) {
							writer.println("There exists a static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream);
							///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
							this.setLastValue(value);
						} else {
							writer.println("There exists no static condition from the data subject " + value.f0
									+ " over the stream " + this.associatedStream
									+ ". Setting no last value associated to the waiting tuple.");
							this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
						}
					}
				} else if (isLast && isFirst) {
					writer.println("This PCC is the only one of the chain.");
					writer.println("Scheduling processing of the received tuple and forwarding.");
					if (this.conditionPerDataSubject.containsKey(value.f0)) {
						writer.println("There exists a static condition from the data subject " + value.f0
								+ " over the stream " + this.associatedStream);
						///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
						this.setLastValue(value);
					} else {
						writer.println("There exists no static condition from the data subject " + value.f0
								+ " over the stream " + this.associatedStream
								+ ". Setting no last value associated to the waiting tuple.");
						this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
					}
					this.eventTimeFlatMap1(value, out);
				} else if (!isLast && isFirst) {
					if (this.conditionPerDataSubject.containsKey(value.f0)) {
						writer.println("There exists a static condition from the data subject " + value.f0
								+ " over the stream " + this.associatedStream);
						///////////////////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
						this.setLastValue(value);
					} else {
						writer.println("There exists no static condition from the data subject " + value.f0
								+ " over the stream " + this.associatedStream
								+ ". Setting no last value associated to the waiting tuple.");
						this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
					}
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

	private void setLastValue(Tuple3<String, T, List<Boolean>> value)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		if (this.lastValuePerSubject.get(value.f0) != null) {
			Field ts = this.lastValuePerSubject.get(value.f0).getClass().getDeclaredField("eventTime");
			ts.setAccessible(true);

			Field e = value.f1.getClass().getDeclaredField("eventTime");
			e.setAccessible(true);

			if ((Long) ts.get(this.lastValuePerSubject.get(value.f0)) < (Long) e.get(value.f1)) {
				writer.println("There is a value about " + value.f0 + " from the stream " + this.associatedStream
						+ ". Setting this as the last value associated to the waiting tuple.");
				this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, this.lastValuePerSubject.get(value.f0)));
			} else {
				writer.println("There is a value about " + value.f0 + " from the stream " + this.associatedStream
						+ ", but it is too new." + " Setting no value associated to the waiting tuple.");
				///////////////////////////////// HERE THE VALUE SHOULD BE PICKED FROM THE
				///////////////////////////////// RETAINED LIST
				///////////////////////////////// WHICH SHOULD ALSO BE UPDATED
				if (this.retainedOtherStreamLastValue.isEmpty()) {
					this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
				} else {
					S last = null;
					for (Tuple2<String, S> t : this.retainedOtherStreamLastValue) {
						if (t.f0.equals(value.f0)) {
							if (last == null) {
								if ((Long) ts.get(t.f1) <= (Long) e.get(value.f1)) {
									last = t.f1;
								}
							} else {
								if ((Long) ts.get(t.f1) <= (Long) e.get(value.f1)
										&& (Long) ts.get(t.f1) > (Long) ts.get(last)) {
									last = t.f1;
								}
							}
						}
					}

					if (last != null) {
						this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, last));
						Iterator<Tuple2<String, S>> iter = this.retainedOtherStreamLastValue.iterator();
						while (iter.hasNext()) {
							Tuple2<String, S> t = iter.next();
							if (t.f0.equals(value.f0)) {
								if ((Long) ts.get(t.f1) < (Long) ts.get(last)) {
									iter.remove();
								}
							}
						}
					} else {
						this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
					}
					/////////////////////////////////
					/////////////////////////////////
				}
			}
		} else {
			writer.println("There is still no value about " + value.f0 + " from the stream " + this.associatedStream
					+ ". Setting no last value associated to the waiting tuple.");
			this.tupleMetadata.put(value.f1, new Tuple3<>(value.f0, null, null));
		}
	}

	private boolean checkCondition(T t, ContextualCondition cond)
			throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {

		S lastTuple;

		if (this.tupleMetadata.get(t) != null) {
			writer.println("The tuple is currently waiting (event time case)");
			lastTuple = this.tupleMetadata.get(t).f2;

		} else {
			writer.println("The tuple is no waiting (processing time case)");

			Field ds = t.getClass().getDeclaredField("dataSubject");
			ds.setAccessible(true);

			lastTuple = this.lastValuePerSubject.get(ds.get(t));
		}

		if (lastTuple != null) {
			Field a = lastTuple.getClass().getDeclaredField(cond.getVariable());
			a.setAccessible(true);
			writer.println("Found tuple from the stream " + this.associatedStream
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
				writer.println("Processing in event time and found no tuple from stream " + this.associatedStream
						+ " associated to the main stream tuple being processed. It should be that when the mainstream tuple arrived there"
						+ "was no available tuple from the other stream e no tuple arrived in the waiting time.");
			} else {
				writer.println("Processing in processing time and found no tuple from stream " + this.associatedStream
						+ " associated to the main stream tuple being processed. It should be that no tuple from the associated stream has been received yet.");
			}
			return false;
		}

	}

	private void initializeWindow(String ds, T tuple, PastCondition cond)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		Field e1 = tuple.getClass().getDeclaredField("eventTime");
		e1.setAccessible(true);

		Field e2 = null;

		// create a new entry in windowPerTuple and populate the list with the current
		// content
		List<Tuple2<String, S>> initWindow = new ArrayList<Tuple2<String, S>>();
		for (Tuple2<String, S> t : retainedOtherStreamWindow) {
			if (e2 == null) {
				e2 = t.f1.getClass().getDeclaredField("eventTime");
				e2.setAccessible(true);
			}
			if (t.f0.equals(ds)) {
				if ((Long) e2.get(t.f1) > (Long) e1.get(tuple) - cond.timeWindowMilliseconds()) {
					initWindow.add(t);
				}
			}
		}
		if (this.lastValuePerSubject.get(ds) != null) {
			this.tupleMetadata.put(tuple, new Tuple3<>(ds, initWindow, this.lastValuePerSubject.get(ds)));
		} else {
			this.tupleMetadata.put(tuple, new Tuple3<>(ds, initWindow, null));
		}

		// flushing from the retainedWindow all the tuples older than the max window in
		// the past specified by the managed past conditions
		PastCondition pc = this.pastConditionPerDataSubject.get(ds);
		java.util.Iterator<Tuple2<String, S>> iter = this.retainedOtherStreamWindow.iterator();
		while (iter.hasNext()) {
			Tuple2<String, S> rt = iter.next();
			if (e2 == null) {
				e2 = rt.f1.getClass().getDeclaredField("eventTime");
				e2.setAccessible(true);
			}
			if (rt.f0.equals(ds) && this.lastValuePerSubject.get(ds) != null) {
				if ((Long) e2.get(rt.f1) < (Long) e2.get(this.lastValuePerSubject.get(ds))
						- pc.getUpperTemporalBound()) {
					iter.remove();
				}
			}
		}
		//
	}

	private void checkTupleCondition(Tuple3<String, T, List<Boolean>> value,
			Collector<Tuple3<String, T, List<Boolean>>> out) throws Exception {

		writer.println("Checking conditions for tuple " + value.f1);

		if (pastConditionPerDataSubject.containsKey(value.f0)) {

			writer.println("There exists a past condition specified by data subject " + value.f0 + " on stream "
					+ this.associatedStream);

			PastCondition userPastEventPolicy = this.pastConditionPerDataSubject.get(value.f0);

			List<Tuple2<String, S>> window = this.tupleMetadata.get(value.f1).f1;

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

			writer.println("The result of the past condition checking is: " + this.resultPerTuple.get(value.f1));
		}

		if (this.isFirst) {
			writer.println("This is the first PCC on the chian, so it is going to trigger the releasing of the tuple.");
			if (this.pastConditionPerDataSubject.containsKey(value.f0)) {
				writer.println(
						"Adding to the tuple the result for the past condition specified by data subject " + value.f0);
				value.f2.add(this.resultPerTuple.get(value.f1));
				this.resultPerTuple.remove(value.f1);
				if (this.processingInEventTime) {
					this.tupleMetadata.remove(value.f1);
				}
			}

			if (this.conditionPerDataSubject.containsKey(value.f0)) {
				writer.println("Adding to the tuple the result for the static condition specified by data subject "
						+ value.f0);
				value.f2.add(this.checkCondition(value.f1, this.conditionPerDataSubject.get(value.f0)));
			}
			
			if(this.processingInEventTime && this.alowedLateness > 0) {
				out.collect(value);
			}
		}

		if (this.otherResults.containsKey(value.f1)) {
			writer.println(
					"This is a PCC for which the timer of the current tuple was already expired and was waiting for the tuple being received for the second time.");
			writer.println("Adding to the tuple all the results that were stored for it.");
			value.f2.add(this.resultPerTuple.get(value.f1));
			for (Boolean result : this.otherResults.get(value.f1)) {
				value.f2.add(result);
			}
			this.tupleMetadata.remove(value.f1);
			this.resultPerTuple.remove(value.f1);

			if (this.conditionPerDataSubject.containsKey(value.f0)) {
				writer.println("Adding to the tuple the result for the static condition specified by data subject "
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
	public void flatMap2(Tuple2<String, S> value, Collector<Tuple3<String, T, List<Boolean>>> out) throws Exception {

		writer.println("Received new tuple on stream " + this.associatedStream + ": " + value.f1);

		Field t1 = value.f1.getClass().getDeclaredField("eventTime");
		t1.setAccessible(true);
		
		Field t2 = null;

		if (pastConditionPerDataSubject.containsKey(value.f0)) {

			writer.println("There exists a past condition for the associated data subject " + value.f0);

			this.retainedOtherStreamWindow.add(value);

			PastCondition userPastEventPolicy = pastConditionPerDataSubject.get(value.f0);

			writer.println("Updating the windows associated to each waiting tuple which referes to " + value.f0);

			for (T tuple : tupleMetadata.keySet()) {
				
				Tuple3<String, List<Tuple2<String, S>>, S> entry = tupleMetadata.get(tuple);
				if(entry != null && entry.f1 != null) {
					List<Tuple2<String, S>> updatedList = entry.f1;

					Field ds = tuple.getClass().getDeclaredField("dataSubject");
					ds.setAccessible(true);
					
					if (t2 == null) {
						t2 = tuple.getClass().getDeclaredField("eventTime");
						t2.setAccessible(true);
					}

					if (value.f0.equals(ds.get(tuple))) {
						writer.println("Found a waiting tuple of the main stream about " + value.f0 + ": " + tuple);
						if ((Long) t1.get(value.f1) >= (Long) t2.get(tuple) - userPastEventPolicy.timeWindowMilliseconds()
								&& (Long) t1.get(value.f1) <= (Long) t2.get(tuple)) {
							writer.println(
									"The just received tuple has a timestamp which is older than the waiting tuple but within the window specified by the past condition. Updating the window associated to the waiting tuple.");
							updatedList.add(value);
						} else {
							writer.println(
									"The just received tuple has a timestamp which is either newer than the waiting tuple or that do not belong to the window specified by the past condition. No update.");
						}
					}
				}
			}
		}

		if (conditionPerDataSubject.containsKey(value.f0)) {

			this.retainedOtherStreamLastValue.add(value);
			writer.println("There exists a static condition for the associated data subject " + value.f0);

			writer.println("Updating the last value associated to each waiting tuple which referes to " + value.f0);
			for (T tuple : this.tupleMetadata.keySet()) {
				if (t2 == null) {
					t2 = tuple.getClass().getDeclaredField("eventTime");
					t2.setAccessible(true);
				}
				if (this.tupleMetadata.get(tuple).f0.equals(value.f0)) {
					writer.println("Found a waiting tuple of the main stream about " + value.f0 + ": " + tuple);
					if (this.tupleMetadata.get(tuple).f2 != null) {
						if ((Long) t1.get(value.f1) > (Long) t2.get(this.tupleMetadata.get(tuple).f2)
								&& (Long) t1.get(value.f1) <= (Long) t2.get(tuple)) {
							writer.println(
									"The just received tuple has a timestamp which is older than the waiting tuple but newer than the currently associated last value. Updating last value associated to the waiting tuple.");
							this.tupleMetadata.put(tuple,
									new Tuple3<>(value.f0, this.tupleMetadata.get(tuple).f1, value.f1));
							writer.println("Updated last value: " + this.tupleMetadata.get(tuple).f2);
						} else {
							writer.println(
									"The just received tuple has a timestamp which is either newer than the waiting tuple or older than the currently associated last value. No update.");
						}
					} else {
						writer.println("There is no previous last value from stream " + this.associatedStream
								+ " associated to the waiting tuple");
						if ((Long) t2.get(value.f1) <= (Long) t2.get(tuple)) {
							writer.println(
									"The just received tuple has a timestamp which is older than the waiting tuple. Updating last value associated to the waiting tuple.");

							this.tupleMetadata.put(tuple,
									new Tuple3<>(value.f0, this.tupleMetadata.get(tuple).f1, value.f1));
						} else {
							writer.println(
									"The just received tuple has a timestamp which is newer than the waiting tuple. No update.");
						}
					}
				}
			}

			writer.println("Updating the most recent value which has been globally seen for stream "
					+ this.associatedStream + " and data subject " + value.f0);
			if (this.lastValuePerSubject.get(value.f0) != null) {
				if (t2 == null) {
					t2 = this.lastValuePerSubject.get(value.f0).getClass().getDeclaredField("eventTime");
					t2.setAccessible(true);
				}
				writer.println("There is already a previous value.");
				if ((Long) t1.get(value.f1) > (Long) t2.get(this.lastValuePerSubject.get(value.f0))) {
					writer.println("The previous value is older than the one just received. Updating.");
					this.lastValuePerSubject.put(value.f0, value.f1);
				} else {
					writer.println("The previous value is newer than the one just received. Non updating.");
				}
			} else {
				writer.println("There is no previous value. Updating");
				this.lastValuePerSubject.put(value.f0, value.f1);
			}
			writer.println("Updated value: " + this.lastValuePerSubject.get(value.f0));

		}
	}

	public void addPastCondition(String dataSubject, PastCondition cond) {
		this.pastConditionPerDataSubject.put(dataSubject, cond);
	}

	public void addStaticCondition(String dataSubject, ContextualCondition cond) {
		this.conditionPerDataSubject.put(dataSubject, cond);
	}

	private Boolean existsEqual(List<Tuple2<String, S>> toControl, PastCondition cond) {

		Field var;

		for (Tuple2<String, S> t : toControl) {
			try {
				var = t.f1.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if (var.get(t.f1).equals(cond.getValue())) {
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

	private Boolean existsGreater(List<Tuple2<String, S>> toControl, PastCondition cond) {
		Field var;

		for (Tuple2<String, S> t : toControl) {
			try {
				var = t.f1.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);

				if ((Integer) var.get(t.f1) > (Integer) cond.getValue()) {
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

	private Boolean existsGreaterOrEqual(List<Tuple2<String, S>> toControl, PastCondition cond) {
		Field var;

		for (Tuple2<String, S> t : toControl) {
			try {
				var = t.f1.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if ((Integer) var.get(t.f1) >= (Integer) cond.getValue()) {
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

	private Boolean existsLess(List<Tuple2<String, S>> toControl, PastCondition cond) {
		Field var;

		for (Tuple2<String, S> t : toControl) {
			try {
				var = t.f1.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if ((Integer) var.get(t.f1) < (Integer) cond.getValue()) {
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

	private Boolean existsLessOrEqual(List<Tuple2<String, S>> toControl, PastCondition cond) {
		Field var;

		for (Tuple2<String, S> t : toControl) {
			try {
				var = t.f1.getClass().getDeclaredField(cond.getVariable());
				var.setAccessible(true);
				if ((Integer) var.get(t.f1) <= (Integer) cond.getValue()) {
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

	private class EventTimeComparator implements Comparator<Tuple2<String, S>> {

		@Override
		public int compare(Tuple2<String, S> a, Tuple2<String, S> b) {
			try {
				Field comparable;
				comparable = a.f1.getClass().getDeclaredField("eventTime");
				comparable.setAccessible(true);
				return (Long) comparable.get(a.f1) < (Long) comparable.get(b.f1) ? -1
						: ((Long) comparable.get(a.f1)).equals((Long) comparable.get(b.f1)) ? 0 : 1;
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
	public void open(Configuration parameters) throws Exception {
		try {
			this.writer = new PrintWriter(
					logDir + "/subject-specific-condition-checker-" + this.associatedStream + ".log", "UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.open(parameters);
	}

	@Override
	public void close() throws Exception {
		scheduler.shutdown();
		this.writer.close();
		super.close();
	}

}
