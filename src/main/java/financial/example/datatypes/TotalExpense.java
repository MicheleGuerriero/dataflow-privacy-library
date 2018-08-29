package financial.example.datatypes;

import java.io.Serializable;

public class TotalExpense implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8425734499727611515L;
	
	private String tupleId;
	
	private static Integer tupleSeqNumber = 1;
	
	public static String getNextTupleId() {
		tupleSeqNumber = tupleSeqNumber + 1;
		return "t" + (tupleSeqNumber -1) ;
	}
	

	public String getTupleId() {
		return tupleId;
	}

	public void setTupleId(String tupleId) {
		this.tupleId = tupleId;
	}

	private String dataSubject;
	
	private Integer totalAmount;
	
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

	public Integer getTotalAmount() {
		return totalAmount;
	}

	public void setTotalAmount(Integer totalAmount) {
		this.totalAmount = totalAmount;
	}
	
}
