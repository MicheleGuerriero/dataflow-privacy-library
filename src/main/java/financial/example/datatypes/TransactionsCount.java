package financial.example.datatypes;

import java.io.Serializable;

public class TransactionsCount implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1212875353556512315L;

	private String dataSubject;

	private Integer nTransactions;

	private Long eventTime;

	private String tupleId;

	private static Integer tupleSeqNumber = 1;

	private static final String streamId = "s2";

	public static String getNextTupleId() {
		tupleSeqNumber = tupleSeqNumber + 1;
		return "t" + (tupleSeqNumber - 1);
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
		sb.append(" " + this.streamId + " (");
		sb.append(this.dataSubject).append(",");
		sb.append(this.nTransactions);
		sb.append(")");

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
