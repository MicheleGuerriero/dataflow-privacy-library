package financial.example.datatypes;

import java.io.Serializable;

public class FinancialTransaction implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 827026187961182328L;

	private String tupleId;
	
	private String dataSubject;
	
	private Integer amount;
	
	private String recipient;
	
	private Long eventTime;
	
    private static final String streamId = "s1";
	
	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();

		sb.append("@" + this.eventTime);
		sb.append(" " + this.streamId + " (");
		sb.append(this.dataSubject).append(",");
		sb.append(this.amount).append(",");
		sb.append(this.recipient);
		sb.append(")");

		return sb.toString();
	}
	
	public Long getEventTime() {
		return eventTime;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}
	
	public FinancialTransaction() {
		
	}
	
	public FinancialTransaction(String transactionId, String dataSubject, Integer amount, String recipient, Long eventTime) {
		this.tupleId = transactionId;
		this.dataSubject = dataSubject;
		this.amount = amount;
		this.recipient = recipient;
		this.eventTime = eventTime;
	}
	
	public String getTupleId() {
		return tupleId;
	}

	public void setTupleId(String tupleId) {
		this.tupleId = tupleId;
	}

	public String getDataSubject() {
		return dataSubject;
	}

	public void setDataSubject(String dataSubject) {
		this.dataSubject = dataSubject;
	}

	public Integer getAmount() {
		return amount;
	}

	public void setAmount(Integer amount) {
		this.amount = amount;
	}

	public String getRecipient() {
		return recipient;
	}

	public void setRecipient(String recipient) {
		this.recipient = recipient;
	}
	
}
