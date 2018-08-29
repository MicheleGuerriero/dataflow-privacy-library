package financial.example.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import financial.example.datatypes.FinancialTransaction;
import financial.example.datatypes.TopConsumersCount;
import financial.example.datatypes.TotalExpense;
import financial.example.datatypes.TransactionsCount;

public class TopConsumersCounter extends RichAllWindowFunction<TotalExpense, TopConsumersCount, TimeWindow> {

	private static final long serialVersionUID = 1017661210567055436L;

	@Override
	public void apply(TimeWindow window, Iterable<TotalExpense> values, Collector<TopConsumersCount> out)
			throws Exception {

		Map<String, Integer> totPerSubject = new HashMap<String, Integer>();
		
		for(TotalExpense t: values) {
			if(totPerSubject.containsKey(t.getDataSubject())) {
				totPerSubject.put(t.getDataSubject(), totPerSubject.get(t.getDataSubject()) + t.getTotalAmount());
			} else {
				totPerSubject.put(t.getDataSubject(), t.getTotalAmount());
			}
		}
		
		Integer count = 0;
		
		for(String ds: totPerSubject.keySet()) {
			if(totPerSubject.get(ds) >= 1000.0) {
				count = count + 1;
			}
		}
		
		TopConsumersCount nTopConsu = new TopConsumersCount();
		
		nTopConsu.setTupleId(TopConsumersCount.getNextTupleId());
		nTopConsu.setCount(count);
		nTopConsu.setEventTime(window.getEnd());

		out.collect(nTopConsu);
	}

}
