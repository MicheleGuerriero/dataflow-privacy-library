package it.deib.polimi.diaprivacy.library;

public interface ConditionChecker <T, S> {

	public void setIsLast(Boolean isLast);
	
	public int getAssociatedStream();
}
