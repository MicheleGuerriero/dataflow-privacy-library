package example1.datatypes;

import java.io.Serializable;
import java.lang.reflect.Field;

public class SubjectSpecific implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String tupleId;
	
	private String dataSubject;

    private Integer content;
    
    private Long eventTime;
    
    private String streamId;
    
    private Integer contentOrg;
 
    public SubjectSpecific() {

    }

    public SubjectSpecific(String dataSubject, Integer content, String tupleId, Boolean isObserved, Long eventTime, String streamId) {
    	this.tupleId = tupleId;
        this.dataSubject = dataSubject;
        this.content = content;
        this.streamId = streamId;
        this.eventTime = eventTime;
        
        if(isObserved) {
        	this.contentOrg = content;
        }
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

	public Integer getContentOrg() {
		return contentOrg;
	}

	public void setContentOrg(Integer contentOrg) {
		this.contentOrg = contentOrg;
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
        sb.append(this.dataSubject).append(",");
        sb.append(this.content);

        if (contentOrg != null){
          sb.append(",").append(contentOrg).append(")");
        } else {
          sb.append(")");
        }
        
        return sb.toString();
    }
    
	@Override
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
