package it.deib.polimi.diaprivacy.library;

import java.io.Serializable;

public class GeneralizationLevel implements Serializable {

	private static final long serialVersionUID = -1442592820010973501L;

	private String variable;
	
	private Integer level;
	
	public GeneralizationLevel() {
		
	}
	
	public GeneralizationLevel(String variable, Integer level) {
		this.variable = variable;
		this.level = level;
		
	}

	public String getVariable() {
		return variable;
	}

	public void setVariable(String variable) {
		this.variable = variable;
	}

	public Integer getLevel() {
		return level;
	}

	public void setLevel(Integer level) {
		this.level = level;
	}
}
