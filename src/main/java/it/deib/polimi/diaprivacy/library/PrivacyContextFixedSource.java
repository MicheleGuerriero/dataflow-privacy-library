package it.deib.polimi.diaprivacy.library;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import it.deib.polimi.diaprivacy.model.PrivacyContext;

public class PrivacyContextFixedSource implements SourceFunction<PrivacyContext> {

	private static final long serialVersionUID = 5558127178703269065L;
	
	private Integer initialDelay;
	private Integer sleepTimeBeforeFinish;
	private List<Tuple2<PrivacyContext, Long>> context;


	public PrivacyContextFixedSource() {
		// TODO Auto-generated constructor stub
	}

	public PrivacyContextFixedSource(Integer initialDelay, Integer sleepTimeBeforeFinish, String fixedUser, String fixedRole, String fixesPurpose) {
		this.initialDelay = initialDelay;
		this.sleepTimeBeforeFinish = sleepTimeBeforeFinish;
		
		this.context = new ArrayList<Tuple2<PrivacyContext, Long>>();
		context.add(new Tuple2<PrivacyContext, Long>(
			new PrivacyContext(fixedUser, fixedRole, fixesPurpose, 800L), 0L));


	}

	@Override
	public void run(SourceContext<PrivacyContext> ctx) throws Exception {


		Thread.sleep(initialDelay);

		for(Tuple2<PrivacyContext, Long> c: context) {
			ctx.collect(c.f0);
			Thread.sleep(c.f1);

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
