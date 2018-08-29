package financial.example;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import financial.example.datatypes.FinancialTransaction;

public class FinancialTransactionFixedSource  implements SourceFunction<FinancialTransaction> {
	private static final long serialVersionUID = 2292045454504629394L;

	private Integer initialDelay;
	private Integer sleepTimeBeforeFinish;

	private List<Tuple2<FinancialTransaction, Long>> workload;

	public FinancialTransactionFixedSource() {

	}

	public FinancialTransactionFixedSource(Integer initialDelay, Integer sleepTimeBeforeFinish, List<Tuple2<FinancialTransaction, Long>> workload) {

		this.initialDelay = initialDelay;
		this.sleepTimeBeforeFinish = sleepTimeBeforeFinish;
		this.workload = workload;
	}

	@Override
	public void run(SourceContext<FinancialTransaction> ctx) throws Exception {

		this.generateStream(ctx);

	}

	private void generateStream(SourceContext<FinancialTransaction> sourceContext)
			throws UnknownHostException, IOException, InterruptedException {

		Thread.sleep(initialDelay);

		for (Tuple2<FinancialTransaction, Long> t : this.workload) {

			sourceContext.collect(t.f0);
			
			Thread.sleep(t.f1);

		}

		Thread.sleep(sleepTimeBeforeFinish);

	}

	@Override
	public void cancel() {

	}
}
