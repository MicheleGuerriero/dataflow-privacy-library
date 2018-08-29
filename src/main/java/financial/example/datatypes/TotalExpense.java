package financial.example.datatypes;

public class TotalExpense {
	
	private String dataSubject;
	
	private Double totalAmount;
	
	private Long eventTime;

    @Override
    public String toString() {
    	
    	StringBuilder sb = new StringBuilder();
    	
        sb.append("@" + this.eventTime);
        sb.append(" " + this.dataSubject + ",");
        sb.append(" " + this.totalAmount);
        
        return sb.toString();
    }
    
	public Long getEventTime() {
		return eventTime;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}

	public TotalExpense() {
		
	}

	public String getDataSubject() {
		return dataSubject;
	}

	public void setDataSubject(String dataSubject) {
		this.dataSubject = dataSubject;
	}

	public Double getTotalAmount() {
		return totalAmount;
	}

	public void setTotalAmount(Double totalAmount) {
		this.totalAmount = totalAmount;
	}
	
}
