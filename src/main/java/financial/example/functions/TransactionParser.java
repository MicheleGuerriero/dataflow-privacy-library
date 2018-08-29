package financial.example.functions;

import org.apache.flink.api.common.functions.RichMapFunction;

import financial.example.datatypes.FinancialTransaction;

public class TransactionParser extends RichMapFunction<String, FinancialTransaction>{

	private static final long serialVersionUID = -8868278591368747029L;

	@Override
	public FinancialTransaction map(String value) throws Exception {
		// TODO Auto-generated method stub
		String[] splits = value.split(",");
		
		FinancialTransaction toReturn = new FinancialTransaction();
		toReturn.setTupleId(splits[0]);
		toReturn.setDataSubject(splits[1]);
		toReturn.setAmount(Integer.parseInt(splits[2]));
		toReturn.setRecipient(splits[3]);
		toReturn.setEventTime(Long.parseLong(splits[4]));
		return toReturn;
	}
	
	

}
