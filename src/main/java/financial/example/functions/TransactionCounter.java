package financial.example.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import financial.example.datatypes.FinancialTransaction;
import financial.example.datatypes.TransactionsCount;

public class TransactionCounter extends RichWindowFunction<FinancialTransaction, TransactionsCount, Tuple, TimeWindow> {

	private static final long serialVersionUID = 3803106678439040457L;

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<FinancialTransaction> input,
			Collector<TransactionsCount> out) throws Exception {
		
		Integer count = 0;
		
		for(FinancialTransaction t: input) {
			count = count + 1;
		}
		
		TransactionsCount toReturn = new TransactionsCount();
		
		toReturn.setTupleId(TransactionsCount.getNextTupleId());
		toReturn.setDataSubject(key.getField(0));
		toReturn.setnTransactions(count);
		toReturn.setEventTime(window.getEnd());

		Thread.sleep(5000);
		
		out.collect(toReturn);
	}

}
