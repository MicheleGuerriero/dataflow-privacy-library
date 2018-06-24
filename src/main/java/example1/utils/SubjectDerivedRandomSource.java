package example1.utils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import example1.datatypes.SubjectDerived;
import example1.datatypes.SubjectSpecific;

public class SubjectDerivedRandomSource implements SourceFunction<SubjectDerived> {

	private Integer initialDelay;
	private Integer nDataSubject;
	private Integer minIntervalBetweenEmit;
	private Integer maxIntervalBetweenEmit;
	private Boolean isNanoseconds;
	private Integer minNumber;
	private Integer maxNumber;
	private Integer nTuples;
	private Integer sleepTimeBeforeFinish;
	private Boolean isObserved;
	private String streamId;
	private Integer warmUpTuples;
	private Integer coolDownTuples;
	private String timestampServerIp;
	private Integer timestampServerPort;
	private Boolean simulateRealisticScenario;
	private Long maxDelayMsecs;
	private Integer minDelay;
	private Integer maxDelay;

	private Boolean started;
	private Boolean coolingDown;
	private Integer tCounter;
	
	private List<Tuple2<SubjectDerived, Long>> workloadQueue;


	public SubjectDerivedRandomSource() {

	}

	public SubjectDerivedRandomSource(Integer initialDelay, Integer nDataSubject, Integer minIntervalBetweenEmit,
			Integer maxIntervalBetweenEmit, Boolean isNanosecond, Integer minNumber, Integer maxNumber, Integer nTuples,
			Integer sleepTimeBeforeFinish, Boolean isObserved, String streamId, Integer warmUpTuples,
			Integer coolDownTuples, String timestampServerIp, Integer timestampServerPort,
			Boolean simulateRealisticScenario, Long maxDelayMsecs, Integer minDelay, Integer maxDelay) {
		this.started = false;
		this.coolingDown = false;
		this.tCounter = 0;

		this.initialDelay = initialDelay;
		this.nDataSubject = nDataSubject;
		this.minIntervalBetweenEmit = minIntervalBetweenEmit;
		this.maxIntervalBetweenEmit = maxIntervalBetweenEmit;
		this.isNanoseconds = isNanosecond;
		this.minNumber = minNumber;
		this.maxNumber = maxNumber;
		this.nTuples = nTuples;
		this.sleepTimeBeforeFinish = sleepTimeBeforeFinish;
		this.isObserved = isObserved;
		this.streamId = streamId;
		this.warmUpTuples = warmUpTuples;
		this.coolDownTuples = coolDownTuples;
		this.timestampServerIp = timestampServerIp;
		this.timestampServerPort = timestampServerPort;
		this.simulateRealisticScenario = simulateRealisticScenario;
		this.maxDelayMsecs = maxDelayMsecs;
		this.minDelay = minDelay;
		this.maxDelay = maxDelay;
		
		this.workloadQueue = new ArrayList<Tuple2<SubjectDerived, Long>>();
	}

	@Override
	public void run(SourceContext<SubjectDerived> ctx) throws Exception {
		if (simulateRealisticScenario) {
			// this.generateUnorderedStream(ctx);
			this.generateDelayedStream(ctx);
		}
		else {
			this.generateOrderedStream(ctx);
		}
	}

	private void generateOrderedStream(SourceContext<SubjectDerived> sourceContext)
			throws UnknownHostException, IOException, InterruptedException {

		Random randomSleep = new Random();
		Random randomAmount = new Random();

		PrintStream socketWriter = new PrintStream(
				new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort).getOutputStream());

		Thread.sleep(initialDelay);

		while (tCounter < nTuples) {
			if (tCounter.equals(warmUpTuples)) {
				socketWriter.println("jobStart");
				this.started = true;
			}

			if (tCounter.equals(nTuples - coolDownTuples)) {
				this.coolingDown = true;
			}

			SubjectDerived data = new SubjectDerived("t" + tCounter,
					minNumber + randomAmount.nextInt(maxNumber - minNumber), System.currentTimeMillis(), streamId);
			sourceContext.collect(data);

			if (this.started && !this.coolingDown) {
				socketWriter.println(data.getTupleId() + "_start");
			}

			tCounter = tCounter + 1;

			if (isNanoseconds) {
				Integer interval = minIntervalBetweenEmit
						+ randomSleep.nextInt((maxIntervalBetweenEmit - minIntervalBetweenEmit));
				Long start = System.nanoTime();
				Long end = 0L;
				do {
					end = System.nanoTime();
				} while (start + interval >= end);
			} else {
				Thread.sleep(minIntervalBetweenEmit
						+ randomSleep.nextInt((maxIntervalBetweenEmit - minIntervalBetweenEmit) + 1));
			}

		}
		Thread.sleep(sleepTimeBeforeFinish);

		socketWriter.close();

	}

	private void generateUnorderedStream(SourceContext<SubjectDerived> sourceContext)
			throws UnknownHostException, IOException, InterruptedException {
		Random randomSleep = new Random();
		Random coin = new Random();
		Random randomAmount = new Random();
		Random randomDataSubject = new Random();

		PrintStream socketWriter = new PrintStream(
				new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort).getOutputStream());

		Thread.sleep(initialDelay);

		SubjectDerived first = new SubjectDerived("t" + tCounter,
				minNumber + randomAmount.nextInt(maxNumber - minNumber), System.currentTimeMillis(), streamId);

		while (tCounter < nTuples) {
			if (isObserved && tCounter.equals(warmUpTuples)) {
				socketWriter.println("jobStart");
				this.started = true;
			}

			if (tCounter.equals(nTuples - coolDownTuples)) {
				this.coolingDown = true;
			}

			if (isNanoseconds) {
				Integer interval = minIntervalBetweenEmit
						+ randomSleep.nextInt((maxIntervalBetweenEmit - minIntervalBetweenEmit));
				Long start = System.nanoTime();
				Long end = 0L;
				do {
					end = System.nanoTime();
				} while (start + interval >= end);
			} else {
				Thread.sleep(minIntervalBetweenEmit
						+ randomSleep.nextInt((maxIntervalBetweenEmit - minIntervalBetweenEmit) + 1));
			}

			SubjectDerived second = new SubjectDerived("t" + tCounter,
					minNumber + randomAmount.nextInt(maxNumber - minNumber), System.currentTimeMillis(), streamId);

			if (coin.nextInt() % 2 == 0) {
				sourceContext.collect(first);
				first = second;
				if (isObserved && started && !coolingDown) {
					socketWriter.println(first.getTupleId() + "_start");
				}
			} else {
				sourceContext.collect(second);
				if (isObserved && started && !coolingDown) {
					socketWriter.println(second.getTupleId() + "_start");
				}
			}

			tCounter = tCounter + 1;

		}

		socketWriter.close();

		Thread.sleep(sleepTimeBeforeFinish);

	}
	
	private void generateDelayedStream(SourceContext<SubjectDerived> sourceContext)
			throws UnknownHostException, IOException, InterruptedException {

		Random randomSleep = new Random();
		Random randomAmount = new Random();
		Random randomDelay = new Random();

		Socket s = new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort);

		PrintStream socketWriter = new PrintStream(s.getOutputStream());

		Thread.sleep(initialDelay);

		if (isObserved) {
			socketWriter.println("jobStart");
		}

		SubjectDerived tmp;

		for (int i = 0; i < nTuples || !this.workloadQueue.isEmpty(); i++) {
			if (i < nTuples) {
				tmp = new SubjectDerived("t" + i + 1, minNumber + randomAmount.nextInt(maxNumber - minNumber),
						System.currentTimeMillis(), streamId);

				this.workloadQueue.add(new Tuple2<SubjectDerived, Long>(tmp,
						new Long(randomDelay.nextInt(maxDelay - minDelay))));

			}

			Iterator<Tuple2<SubjectDerived, Long>> iter = this.workloadQueue.iterator();
			
			while(iter.hasNext()) {
				Tuple2<SubjectDerived, Long> t = iter.next();
				if (System.currentTimeMillis() >= t.f0.getEventTime() + t.f1) {
					sourceContext.collect(t.f0);

					if (isObserved) {
						socketWriter.println(t.f0.getTupleId() + "_start");
					}
					
					iter.remove();
				}
			}
			for (Tuple2<SubjectDerived, Long> t : this.workloadQueue) {
	
			}

			if (i < nTuples) {
				if (isNanoseconds) {
					Integer interval = minIntervalBetweenEmit
							+ randomSleep.nextInt((maxIntervalBetweenEmit - minIntervalBetweenEmit));
					Long start = System.nanoTime();
					Long end = 0L;
					do {
						end = System.nanoTime();
					} while (start + interval >= end);
				} else {
					Thread.sleep(minIntervalBetweenEmit
							+ randomSleep.nextInt((maxIntervalBetweenEmit - minIntervalBetweenEmit) + 1));
				}
			}
		}

		Thread.sleep(sleepTimeBeforeFinish);

		s.close();

	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}
}