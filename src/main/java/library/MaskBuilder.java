package library;

import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

import it.deib.polimi.diaprivacy.model.PrivacyContext;
import it.deib.polimi.diaprivacy.model.GeneralizationVector;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MaskBuilder<T> extends PolicyActuator<T> {

	private static final long serialVersionUID = 5567870391226738600L;

	private Map<String, GeneralizationVector> generalizationVectors;

	// to check how this is built up
	private Map<GeneralizationLevel, GeneralizationFunction> generalizationHierarchy;

	public MaskBuilder() {
		super();
		this.generalizationVectors = new HashMap<String, GeneralizationVector>();
		this.generalizationHierarchy = new HashMap<GeneralizationLevel, GeneralizationFunction>();
	}

	public MaskBuilder(String timestampServerIp, Integer timestampServerPort) {
		super(timestampServerIp, timestampServerPort);
		this.generalizationVectors = new HashMap<String, GeneralizationVector>();
		this.generalizationHierarchy = new HashMap<GeneralizationLevel, GeneralizationFunction>();
	}

	@Override
	public void flatMap1(Tuple3<String, T, List<Boolean>> value, Collector<T> out) throws Exception {
		if (this.generalizationVectors.containsKey(value.f0)) {
			if (value.f2.isEmpty()) {
				if (super.matchContext(value.f0, value.f1)) {
					GeneralizationVector gv = this.generalizationVectors.get(value.f0);
					for (String a : gv.getVector().keySet()) {
						Field field = value.f1.getClass().getDeclaredField(a);
						field.setAccessible(true);
						Object finalVal = field.get(value.f1);
						for (int i = 1; i < gv.getVariableGenLevel(a); i++) {
							finalVal = this.getGeneralizationFunction(a, i).apply(finalVal);
						}
						field.set(value.f1, finalVal);
					}
				}
			} else {
				if (this.privacyContextPreferences.containsKey(value.f0)) {
					if (this.internalFold(value.f2, true) && this.matchContext(value.f0, value.f1)) {
						GeneralizationVector gv = this.generalizationVectors.get(value.f0);
						for (String a : gv.getVector().keySet()) {
							Field field = value.f1.getClass().getDeclaredField(a);
							field.setAccessible(true);
							Object finalVal = field.get(value.f1);

							for (int i = 1; i <= gv.getVariableGenLevel(a); i++) {
								finalVal = this.getGeneralizationFunction(a, i).apply(finalVal);
							}
							field.set(value.f1, finalVal);
						}
					}
				} else {
					if (this.internalFold(value.f2, true)) {
						GeneralizationVector gv = this.generalizationVectors.get(value.f0);
						for (String a : gv.getVector().keySet()) {
							Field field = value.f1.getClass().getDeclaredField(a);
							field.setAccessible(true);
							Object finalVal = field.get(value.f1);
							for (int i = 1; i < gv.getVariableGenLevel(a); i++) {
								finalVal = this.getGeneralizationFunction(a, i).apply(finalVal);
							}
							field.set(value.f1, finalVal);
						}
					}
				}
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

	public Map<String, GeneralizationVector> getGeneralizationVectors() {
		return generalizationVectors;
	}

	public void setGeneralizationVectors(Map<String, GeneralizationVector> generalizationVectors) {
		this.generalizationVectors = generalizationVectors;
	}

	public void setGeneralizationVector(String dataSubject, GeneralizationVector v) {
		this.generalizationVectors.put(dataSubject, v);
	}

	public Map<GeneralizationLevel, GeneralizationFunction> getGeneralizationHierarchy() {
		return generalizationHierarchy;
	}

	public void setGeneralizationHierarchy(Map<GeneralizationLevel, GeneralizationFunction> generalizationHierarchies) {
		this.generalizationHierarchy = generalizationHierarchies;
	}

	public void setGeneralizationLevel(GeneralizationLevel level, GeneralizationFunction genFunction) {
		this.generalizationHierarchy.put(level, genFunction);
	}

	public void setGeneralizationLevel(String variable, Integer level, GeneralizationFunction genFunction) {
		this.generalizationHierarchy.put(new GeneralizationLevel(variable, level), genFunction);
	}

	public GeneralizationFunction getGeneralizationFunction(String variable, Integer level) {
		for (GeneralizationLevel l : this.generalizationHierarchy.keySet()) {
			if (l.getVariable().equals(variable) && l.getLevel().equals(level)) {
				return this.getGeneralizationHierarchy().get(l);
			}
		}
		return null;
	}

	private Boolean internalFold(List<Boolean> toFold, Boolean base) {
		Boolean result = base;
		for (Boolean b : toFold) {
			result = result && b;
		}

		return result;
	}

}
