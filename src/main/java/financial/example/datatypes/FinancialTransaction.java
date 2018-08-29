package financial.example.datatypes;

public class FinancialTransaction {

	private String transactionId;
	
	private String dataSubject;
	
	private Double amount;
	
	private String recipient;
	
	private Long eventTime;
	
    @Override
    public String toString() {
    	
    	StringBuilder sb = new StringBuilder();
    	
        sb.append("@" + this.eventTime);
        sb.append(" " + this.transactionId + ",");
        sb.append(" " + this.dataSubject + ",");
        sb.append(" " + this.amount + ",");
        sb.append(" " + this.recipient + "");
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
	
	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getDataSubject() {
		return dataSubject;
	}

	public void setDataSubject(String dataSubject) {
		this.dataSubject = dataSubject;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public String getRecipient() {
		return recipient;
	}

	public void setRecipient(String recipient) {
		this.recipient = recipient;
	}
	
}
