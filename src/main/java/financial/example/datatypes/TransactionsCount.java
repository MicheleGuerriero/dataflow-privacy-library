package financial.example.datatypes;

public class TransactionsCount {

	private String dataSubject;
	
	private Integer nTransactions;
	
	private Long eventTime;
	
    @Override
    public String toString() {
    	
    	StringBuilder sb = new StringBuilder();
    	
        sb.append("@" + this.eventTime);
        sb.append(" " + this.dataSubject + ",");
        sb.append(" " + this.nTransactions);
        
        return sb.toString();
    }
	
	public Long getEventTime() {
		return eventTime;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}

	public String getDataSubject() {
		return dataSubject;
	}

	public void setDataSubject(String dataSubject) {
		this.dataSubject = dataSubject;
	}

	public Integer getnTransactions() {
		return nTransactions;
	}

	public void setnTransactions(Integer nTransactions) {
		this.nTransactions = nTransactions;
	}

	public TransactionsCount() {
		
	}
	
}
