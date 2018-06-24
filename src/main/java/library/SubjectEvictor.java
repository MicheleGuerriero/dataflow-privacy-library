package library;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
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
		
		if (value.f2.isEmpty()) {
			if (super.matchContext(value.f0, value.f1)) {
				out.collect(value.f1);
			}
		} else {
			if (this.internalFold(value.f2, true)) {
				out.collect(value.f1);
			}
		}
		
		Field tId = value.f1.getClass().getDeclaredField("tupleId");
		tId.setAccessible(true);
		out.collect(value.f1);
		PrintStream socketWriter = new PrintStream(socket.getOutputStream());
		socketWriter.println(tId.get(value.f1) + "_end");

	}

	@Override
	public void flatMap2(PrivacyContext value, Collector<T> out) throws Exception {
		this.currentContext = value;
	}

	private Boolean internalFold(List<Boolean> toFold, Boolean base) {
		Boolean result = base;
		for (Boolean b : toFold) {
			result = result && b;
		}

		return result;
	}

}
