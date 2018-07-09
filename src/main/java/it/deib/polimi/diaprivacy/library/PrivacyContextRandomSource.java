package it.deib.polimi.diaprivacy.library;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import it.deib.polimi.diaprivacy.model.PrivacyContext;

public class PrivacyContextRandomSource implements SourceFunction<PrivacyContext> {

	private static final long serialVersionUID = 5558127178703269065L;
	
	private Integer initialDelay;
	private Integer minIntervalBetweenContextSwitch;
	private Integer maxIntervalBetweenContextSwitch;
	private Integer nSwitch;
	private Integer sleepTimeBeforeFinish;

	private ArrayList<String> purposes = new ArrayList<String>() {
		{
			add("marketing");
		}
	};

	private ArrayList<String> roles = new ArrayList<String>() {
		{
			add("employee");
		}
	};

	private ArrayList<String> userIds = new ArrayList<String>() {
		{
			add("u1");
		}
	};

	public PrivacyContextRandomSource() {
		// TODO Auto-generated constructor stub
	}

	public PrivacyContextRandomSource(Integer initialDelay, Integer minIntervalBetweenContextSwitch,
			Integer maxIntervalBetweenContextSwitch, Integer nSwitch, Integer sleepTimeBeforeFinish) {
		this.initialDelay = initialDelay;
		this.minIntervalBetweenContextSwitch = minIntervalBetweenContextSwitch;
		this.maxIntervalBetweenContextSwitch = maxIntervalBetweenContextSwitch;
		this.nSwitch = nSwitch;
		this.sleepTimeBeforeFinish = sleepTimeBeforeFinish;

	}

	@Override
	public void run(SourceContext<PrivacyContext> ctx) throws Exception {

		Random randomSleep = new Random();

		Integer cs = 0;

		Thread.sleep(initialDelay);

		while (cs < nSwitch) {
			String user = this.shuffleList(userIds).get(0);
			String role = this.shuffleList(roles).get(0);
			String purpose = this.shuffleList(purposes).get(0);

			PrivacyContext c = new PrivacyContext(user, role, purpose, System.currentTimeMillis());

			ctx.collect(c);

			cs = cs + 1;

			Thread.sleep(randomSleep.nextInt((maxIntervalBetweenContextSwitch - minIntervalBetweenContextSwitch) + 1));

		}
		Thread.sleep(sleepTimeBeforeFinish);
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

	public static List<String> shuffleList(List<String> a) {
		int n = a.size();
		Random random = new Random();
		random.nextInt();
		for (int i = 0; i < n; i++) {
			int change = i + random.nextInt(n - i);
			swap(a, i, change);
		}

		return a;
	}

	private static void swap(List<String> a, int i, int change) {
		String helper = a.get(i);
		a.set(i, a.get(change));
		a.set(change, helper);
	}

}
