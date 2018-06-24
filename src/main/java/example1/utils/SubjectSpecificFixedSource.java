package example1.utils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import example1.datatypes.SubjectSpecific;

public class SubjectSpecificFixedSource implements SourceFunction<SubjectSpecific> {

	private static final long serialVersionUID = 2292045454504629394L;

	private Integer initialDelay;
	private Integer sleepTimeBeforeFinish;
	private Boolean isObserved;
	private String streamId;
	private String timestampServerIp;
	private Integer timestampServerPort;

	private List<Tuple2<SubjectSpecific, Long>> workload;

	public SubjectSpecificFixedSource() {

	}

	public SubjectSpecificFixedSource(Integer initialDelay, Integer sleepTimeBeforeFinish, Boolean isObserved,
			String streamId, String timestampServerIp, Integer timestampServerPort, Boolean simulateRealisticScenario, List<Tuple2<SubjectSpecific, Long>> workload) {

		this.initialDelay = initialDelay;
		this.sleepTimeBeforeFinish = sleepTimeBeforeFinish;
		this.isObserved = isObserved;
		this.streamId = streamId;
		this.timestampServerIp = timestampServerIp;
		this.timestampServerPort = timestampServerPort;
		this.workload = workload;
	}

	@Override
	public void run(SourceContext<SubjectSpecific> ctx) throws Exception {

		this.generateStream(ctx);

	}

	private void generateStream(SourceContext<SubjectSpecific> sourceContext)
			throws UnknownHostException, IOException, InterruptedException {

		Socket s = new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort);

		PrintStream socketWriter = new PrintStream(s.getOutputStream());

		Thread.sleep(initialDelay);

		if (isObserved) {
			socketWriter.println("jobStart");
		}

		for (Tuple2<SubjectSpecific, Long> t : this.workload) {

			sourceContext.collect(t.f0);

			if (isObserved) {
				socketWriter.println(t.f0.getTupleId() + "_start");
			}

			Thread.sleep(t.f1);

		}

		Thread.sleep(sleepTimeBeforeFinish);

		s.close();

	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

}
