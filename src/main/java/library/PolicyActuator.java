package library;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;

import it.deib.polimi.diaprivacy.model.ContextualCondition;
import it.deib.polimi.diaprivacy.model.PrivacyContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public abstract class PolicyActuator<T>
		extends RichCoFlatMapFunction<Tuple3<String, T, List<Boolean>>, PrivacyContext, T> {

	private static final long serialVersionUID = -726263758475985730L;

	protected Map<String, PrivacyContext> privacyContextPreferences;

	protected Map<String, List<ContextualCondition>> protectedStreamPreferences;
	
	protected Map<String, Map<Integer, List<ContextualCondition>>> staticConditions;
	
	protected Map<String, Map<Integer, Object>> lastStaticStreamValuePerSubject;

	protected PrivacyContext currentContext;

	protected String timestampServerIp;

	protected Integer timestampServerPort;

	protected Socket socket;
		
	@Override
	public void open(Configuration parameters) throws InterruptedException {
		try {
			this.socket = new Socket(InetAddress.getByName(timestampServerIp), timestampServerPort);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws Exception {
		this.socket.close();
		super.close();
	}
	
	public PolicyActuator() {
		this.currentContext = new PrivacyContext();
		this.privacyContextPreferences = new HashMap<String, PrivacyContext>();
		this.protectedStreamPreferences = new HashMap<String, List<ContextualCondition>>();
	}

	public PolicyActuator(String timestampServerIp, Integer timestampServerPort) {
		this.currentContext = new PrivacyContext();
		this.privacyContextPreferences = new HashMap<String, PrivacyContext>();
		this.protectedStreamPreferences = new HashMap<String, List<ContextualCondition>>();
		this.timestampServerIp = timestampServerIp;
		this.timestampServerPort = timestampServerPort;
	}

	protected boolean matchContext(String dataSubject, T current)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

		Boolean r = true;
		if (this.privacyContextPreferences.containsKey(dataSubject) && currentContext != null) {
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
		}

		return r;
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

}
