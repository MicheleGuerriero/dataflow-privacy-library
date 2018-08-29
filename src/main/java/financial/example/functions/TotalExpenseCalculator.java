package financial.example.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import financial.example.datatypes.FinancialTransaction;
import financial.example.datatypes.TotalExpense;
import financial.example.datatypes.TransactionsCount;


public class TotalExpenseCalculator extends RichWindowFunction<FinancialTransaction, TotalExpense, Tuple, TimeWindow> {

	private static final long serialVersionUID = -5452600561298655507L;

	@Override
	public void apply(Tuple key, TimeWindow window, Iterable<FinancialTransaction> input, Collector<TotalExpense> out)
			throws Exception {
		Integer tot = 0;
		
		for(FinancialTransaction t: input) {
			tot = tot + t.getAmount();
		}
		
		TotalExpense toReturn = new TotalExpense();
		
		toReturn.setTupleId(TotalExpense.getNextTupleId());
		toReturn.setDataSubject(key.getField(0));
		toReturn.setTotalAmount(tot);
		toReturn.setEventTime(window.getEnd());

		out.collect(toReturn);		
	}

}
