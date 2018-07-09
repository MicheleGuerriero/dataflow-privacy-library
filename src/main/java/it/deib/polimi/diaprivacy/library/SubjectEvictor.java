package it.deib.polimi.diaprivacy.library;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.GeneralizationVector;
import it.deib.polimi.diaprivacy.model.PrivacyContext;

public class SubjectEvictor<T> extends PolicyActuator<T> {

	private static final long serialVersionUID = 675823976949472258L;

	public SubjectEvictor() {
		this.currentContext = new PrivacyContext();
		this.privacyContextPreferences = new HashMap<String, PrivacyContext>();
		this.protectedStreamPreferences = new HashMap<String, List<ContextualCondition>>();
	}

	public SubjectEvictor(String timestampServerIp, Integer timestampServerPort) {
		this.currentContext = new PrivacyContext();
		this.privacyContextPreferences = new HashMap<String, PrivacyContext>();
		this.protectedStreamPreferences = new HashMap<String, List<ContextualCondition>>();

		this.timestampServerIp = timestampServerIp;
		this.timestampServerPort = timestampServerPort;
	}

	@Override
	public void flatMap1(Tuple3<String, T, List<Boolean>> value, Collector<T> out) throws Exception {
		
		boolean result = false;
		if (value.f2.isEmpty()) {
			if (this.privacyContextPreferences.containsKey(value.f0)) {
				if (this.matchContext(value.f0, value.f1)) {
					result = true;
				}
			}
		} else {
			if (this.privacyContextPreferences.containsKey(value.f0)) {
				if (this.internalFold(value.f2, true) && this.matchContext(value.f0, value.f1)) {
					result = true;
				}
			} else {
				if (this.internalFold(value.f2, true)) {
					result = true;
				}
			}
		}
		
		if(!result) {
			out.collect(value.f1);
		}

	}

}
