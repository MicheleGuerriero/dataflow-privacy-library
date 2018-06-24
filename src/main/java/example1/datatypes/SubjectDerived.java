package example1.datatypes;

import java.io.Serializable;
import java.lang.reflect.Field;

public class SubjectDerived implements Serializable{

	private static final long serialVersionUID = -3980543964417265754L;

	private String tupleId;
	
    private Integer content;
    
    private Long eventTime;
    
    private String streamId;
 
    public SubjectDerived() {

    }

    public SubjectDerived(String tupleId, Integer content, Long eventTime, String streamId) {
    	this.tupleId = tupleId;
        this.content = content;
        this.eventTime = eventTime;
        this.streamId = streamId;
    }

    public Long getEventTime() {
		return eventTime;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

	public String getTupleId() {
		return tupleId;
	}

	public void setTupleId(String tupleId) {
		this.tupleId = tupleId;
	}

	public Integer getContent() {
        return content;
    }

    public void setContent(Integer content) {
        this.content = content;
    }
 
    @Override
    public String toString() {
    	
    	StringBuilder sb = new StringBuilder();
    	
        sb.append("@" + this.eventTime);
        sb.append(" " + this.streamId+ " (");
        sb.append(this.content).append(")");
        
        return sb.toString();
    }
    
	public int hashCode() {
		return this.tupleId.hashCode();
	}
	
	@Override
	public boolean equals(Object other) {
		Field t;
		try {
			t = other.getClass().getDeclaredField("tupleId");
			t.setAccessible(true);
			return this.tupleId.equals((String) t.get(other));
		} catch (NoSuchFieldException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;

	}
	
	
}
