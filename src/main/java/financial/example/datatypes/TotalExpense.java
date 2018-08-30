package financial.example.datatypes;

import java.io.Serializable;

public class TotalExpense implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8425734499727611515L;
	
	private String tupleId;
	
	private static Integer tupleSeqNumber = 1;
	
    private static final String streamId = "s3";
	
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
	
	private Integer totalAmountOrg;
	
	public Integer getTotalAmountOrg() {
		return totalAmountOrg;
	}


	public void setTotalAmountOrg(Integer totalAmountOrg) {
		this.totalAmountOrg = totalAmountOrg;
	}

	private Long eventTime;

    @Override
    public String toString() {
    	
    	StringBuilder sb = new StringBuilder();
    	
        sb.append("@" + this.eventTime);
        sb.append(" " + this.streamId+ " (");
        sb.append(this.dataSubject).append(",");
        sb.append(this.totalAmount);

        if (totalAmountOrg != null){
          sb.append(",").append(totalAmountOrg).append(")");
        } else {
          sb.append(")");
        }
        
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
		if(this.totalAmount == null) {
			this.totalAmount = totalAmount;
			this.totalAmountOrg = totalAmount;
		} else {
			this.totalAmount = totalAmount;
		}
	}
	
}
