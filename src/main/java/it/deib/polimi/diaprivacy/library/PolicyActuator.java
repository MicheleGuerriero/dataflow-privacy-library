package it.deib.polimi.diaprivacy.library;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.GeneralizationVector;
import it.deib.polimi.diaprivacy.model.PrivacyContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public class PolicyActuator<T> extends RichCoFlatMapFunction<Tuple3<String, T, List<Boolean>>, PrivacyContext, T> {

	private static final long serialVersionUID = -726263758475985730L;

	private Map<String, GeneralizationVector> generalizationVectors;

	private Set<String> dsWithEvictionPolicy;

	// to check how this is built up
	private Map<GeneralizationLevel, GeneralizationFunction> generalizationHierarchy;

	protected Map<String, PrivacyContext> privacyContextPreferences;

	protected Map<String, List<ContextualCondition>> protectedStreamPreferences;

	protected PrivacyContext currentContext;

	protected String timestampServerIp;

	protected Integer timestampServerPort;

	protected boolean monitoringActive;

	protected Socket socket;

	public PolicyActuator() {
		this.generalizationVectors = new HashMap<String, GeneralizationVector>();
		this.dsWithEvictionPolicy = new HashSet<String>();
		this.generalizationHierarchy = new HashMap<GeneralizationLevel, GeneralizationFunction>();
		this.currentContext = null;
		this.privacyContextPreferences = new HashMap<String, PrivacyContext>();
		this.protectedStreamPreferences = new HashMap<String, List<ContextualCondition>>();
	}

	public PolicyActuator(String timestampServerIp, Integer timestampServerPort, boolean monitoringActive) {
		this.monitoringActive = monitoringActive;
		this.timestampServerIp = timestampServerIp;
		this.timestampServerPort = timestampServerPort;
		this.generalizationVectors = new HashMap<String, GeneralizationVector>();
		this.dsWithEvictionPolicy = new HashSet<String>();
		this.generalizationHierarchy = new HashMap<GeneralizationLevel, GeneralizationFunction>();
		this.currentContext = new PrivacyContext();
		this.privacyContextPreferences = new HashMap<String, PrivacyContext>();
		this.protectedStreamPreferences = new HashMap<String, List<ContextualCondition>>();

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

	@Override
	public void flatMap1(Tuple3<String, T, List<Boolean>> value, Collector<T> out) throws Exception {
		if (this.generalizationVectors.containsKey(value.f0)) {
			System.out.println("Building mask for tuple: " + value.f1);
			if (value.f2.isEmpty()) {
				System.out.println("The policy specified by data subject " + value.f0
						+ "does not contain any condition on other application streams.");
				if (this.privacyContextPreferences.containsKey(value.f0)) {
					System.out.println(
							"The policy speified by data subject " + value.f0 + " contains a privacy context.");
					if (this.matchContext(value.f0, value.f1)) {
						System.out.println("The specified privacy context currently holds. Generalizing");
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
					} else {
						System.out.println("The specified privayc context currently do not hold.");
					}
				} else {
					System.out.println("There is also no privacy context attached!! (should never go here)");
				}
			} else {
				System.out.println("The policy specified by data subject " + value.f0
						+ " contains some condition on other application streams.");
				if (this.privacyContextPreferences.containsKey(value.f0)) {
					System.out.println(
							"The policy speified by data subject " + value.f0 + " contains a privacy context.");
					System.out.println(this.internalFold(value.f2, true));
					System.out.println(this.matchContext(value.f0, value.f1));
					if (this.internalFold(value.f2, true) && this.matchContext(value.f0, value.f1)) {
						System.out.println("All the conditions match. Generalizing.");
						GeneralizationVector gv = this.generalizationVectors.get(value.f0);
						for (String a : gv.getVector().keySet()) {
							Field field = value.f1.getClass().getDeclaredField(a);
							field.setAccessible(true);
							Object finalVal = field.get(value.f1);
							for (int i = 1; i <= gv.getVariableGenLevel(a); i++) {
								finalVal = this.getGeneralizationFunction(a, i).apply(finalVal);
							}
							System.out.println(finalVal);
							field.set(value.f1, finalVal);
							System.out.println(value.f1);
						}
					} else {
						System.out.println("Some condition does not match. No generalization.");
					}
				} else {
					System.out.println(
							"The policy speified by data subject " + value.f0 + " does not contain a privacy context.");
					if (this.internalFold(value.f2, true)) {
						System.out.println("The condtions on the other streams match. Generalizing.");
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
					} else {
						System.out.println("Some condition on the other stream does not match. No generalization.");
					}
				}
			}

			Field tId = value.f1.getClass().getDeclaredField("tupleId");
			tId.setAccessible(true);
			out.collect(value.f1);
			if (this.monitoringActive) {
				PrintStream socketWriter = new PrintStream(socket.getOutputStream());
				socketWriter.println(tId.get(value.f1) + "_end");
			}

		} else if (this.dsWithEvictionPolicy.contains(value.f0)) {
			System.out.println("Deciding if to tuple " + value.f1 + " has to be evicted or not.");
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

			Field tId = value.f1.getClass().getDeclaredField("tupleId");
			tId.setAccessible(true);
			if (!result) {
				out.collect(value.f1);
			}
			
			if (this.monitoringActive) {
				PrintStream socketWriter = new PrintStream(socket.getOutputStream());
				socketWriter.println(tId.get(value.f1) + "_end");
			}
		} else {
			out.collect(value.f1);
			if (this.monitoringActive) {
				Field tId = value.f1.getClass().getDeclaredField("tupleId");
				tId.setAccessible(true);
				PrintStream socketWriter = new PrintStream(socket.getOutputStream());
				socketWriter.println(tId.get(value.f1) + "_end");
			}
		}
	}

	@Override
	public void open(Configuration parameters) throws InterruptedException {
		if (this.monitoringActive) {
			try {
				this.socket = new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void flatMap2(PrivacyContext value, Collector<T> out) throws Exception {
		this.currentContext = value;
	}

	@Override
	public void close() throws Exception {
		if (this.monitoringActive) {
			this.socket.close();
		}
		super.close();
	}

	protected boolean matchContext(String dataSubject, T current)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		if (!this.privacyContextPreferences.isEmpty() && this.privacyContextPreferences.containsKey(dataSubject)
				&& this.privacyContextPreferences.get(dataSubject) != null && currentContext != null) {
			Boolean r = true;

			PrivacyContext toMatchPrivacyContext = this.privacyContextPreferences.get(dataSubject);
			List<ContextualCondition> toMatchProtectedStreamConds = this.protectedStreamPreferences.get(dataSubject);

			if (toMatchPrivacyContext.getPurpose() != null) {
				r = r && toMatchPrivacyContext.getPurpose().equals(currentContext.getPurpose());
			}

			if (toMatchPrivacyContext.getRole() != null) {
				r = r && toMatchPrivacyContext.getRole().equals(currentContext.getRole());
			}

			if (toMatchPrivacyContext.getUserId() != null) {
				r = r && toMatchPrivacyContext.getUserId().equals(currentContext.getUserId());
			}

			if (toMatchProtectedStreamConds != null) {
				for (ContextualCondition c : toMatchProtectedStreamConds) {
					Field field = current.getClass().getDeclaredField(c.getVariable());
					field.setAccessible(true);
					if (field.get(current) != null) {
						switch (c.getOperator()) {
						case EQUAL:
							r = r && field.get(current).equals(c.getValue());
							break;
						case NOT_EQUAL:
							r = r && !field.get(current).equals(c.getValue());
							break;
						case GREATER:
							r = (r && (Integer) field.get(current) > (Integer) c.getValue());
							break;
						case GREATER_OR_EQUAL:
							r = r && (Integer) field.get(current) >= (Integer) c.getValue();
							break;
						case LESS:
							r = r && (Integer) field.get(current) < (Integer) c.getValue();
							break;
						case LESS_OR_EQUAL:
							r = r && (Integer) field.get(current) <= (Integer) c.getValue();
							break;
						}
					}

				}
			}
			return r;
		} else {
			return true;
		}

	}

	public PrivacyContext getCurrentContext() {
		return currentContext;
	}

	public void setCurrentContext(PrivacyContext currentContext) {
		this.currentContext = currentContext;
	}

	public Map<String, PrivacyContext> getPrivacyContextPreferences() {
		return privacyContextPreferences;
	}

	public void setPrivacyContextPreferences(Map<String, PrivacyContext> privacyContextPreferences) {
		this.privacyContextPreferences = privacyContextPreferences;
	}

	public void setPrivacyContextPreference(String dataSubject, PrivacyContext ctx) {
		this.privacyContextPreferences.put(dataSubject, ctx);
	}

	public Map<String, List<ContextualCondition>> getProtectedStreamPreferences() {
		return protectedStreamPreferences;
	}

	public void setProtectedStreamPreferences(Map<String, List<ContextualCondition>> protectedStreamPreferences) {
		this.protectedStreamPreferences = protectedStreamPreferences;
	}

	public void setProtectedStreamPreference(String dataSubject, ContextualCondition ctx) {
		if (this.protectedStreamPreferences.containsKey(dataSubject)) {
			this.protectedStreamPreferences.get(dataSubject).add(ctx);
		} else {
			List<ContextualCondition> newConds = new ArrayList<ContextualCondition>();
			newConds.add(ctx);
			this.protectedStreamPreferences.put(dataSubject, newConds);
		}

	}

	protected Boolean internalFold(List<Boolean> toFold, Boolean base) {
		Boolean result = base;
		for (Boolean b : toFold) {
			result = result && b;
		}

		return result;
	}

	public Set<String> getDsWithEvictionPolicy() {
		return dsWithEvictionPolicy;
	}

	public void setDsWithEvictionPolicy(Set<String> dsWithEvictionPolicy) {
		this.dsWithEvictionPolicy = dsWithEvictionPolicy;
	}

	public void addDsWithEvictionPolicy(String ds) {
		this.dsWithEvictionPolicy.add(ds);
	}

}
