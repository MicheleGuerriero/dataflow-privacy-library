package financial.example.datatypes;

import java.io.Serializable;

public class TopConsumersCount implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3463443786198544413L;

	private Integer count;
	
	private Long eventTime;
	
	private String tupleId;
	
	private static Integer tupleSeqNumber = 1;
	
    private static final String streamId = "s4";
	
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


    @Override
    public String toString() {
    	
    	StringBuilder sb = new StringBuilder();
    	
        sb.append("@" + this.eventTime);
        sb.append(" " + this.streamId+ " (");
        sb.append(this.count).append(")");
        
        return sb.toString();
    }
    
	public Long getEventTime() {
		return eventTime;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}

	public TopConsumersCount() {
		
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}
	

}
