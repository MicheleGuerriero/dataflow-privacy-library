package example1.utils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import example1.datatypes.SubjectDerived;

public class SubjectDerivedFixedSource implements SourceFunction<SubjectDerived> {

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

	private Boolean started;
	private Boolean coolingDown;
	private Integer tCounter;

	public SubjectDerivedFixedSource() {

	}

	public SubjectDerivedFixedSource(Integer initialDelay, Integer nDataSubject, Integer minIntervalBetweenEmit,
			Integer maxIntervalBetweenEmit, Boolean isNanosecond, Integer minNumber, Integer maxNumber, Integer nTuples,
			Integer sleepTimeBeforeFinish, Boolean isObserved, String streamId, Integer warmUpTuples,
			Integer coolDownTuples, String timestampServerIp, Integer timestampServerPort,
			Boolean simulateRealisticScenario, Long maxDelayMsecs) {
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
	}

	@Override
	public void run(SourceContext<SubjectDerived> ctx) throws Exception {
		if (simulateRealisticScenario)
			this.generateUnorderedStream(ctx);
		else
			this.generateOrderedStream(ctx);
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

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}
}