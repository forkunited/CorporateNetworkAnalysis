package corp.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import corp.net.util.JSONUtil;
import corp.net.util.MathUtil;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class CorpNetEdge extends CorpNetObject {
	private String node1;
	private String node2;
	
	private Map<String, Double> forwardP;
	private Map<String, Double> backwardP;
	private int forwardCount = 0;
	private int backwardCount = 0;
	private List<CorpNetEdgeSource> sources;
	
	public CorpNetEdge(String net, String node1, String node2) {
		this.net = net;
		this.node1 = node1;
		this.node2 = node2;
		
		this.forwardP = new TreeMap<String, Double>();
		this.backwardP = new TreeMap<String, Double>();
		this.forwardCount = 0;
		this.backwardCount = 0;
		this.sources = new ArrayList<CorpNetEdgeSource>();
	}
	
	public JSONObject getJSONAggregate() {
		JSONObject forwardPObj = JSONObject.fromObject(this.forwardP);
		JSONObject forwardObj = new JSONObject();
		forwardObj.put("p", forwardPObj);
		forwardObj.put("count", this.forwardCount);
		
		JSONObject backwardPObj = JSONObject.fromObject(this.backwardP);
		JSONObject backwardObj = new JSONObject();
		backwardObj.put("p", backwardPObj);
		backwardObj.put("count", this.backwardCount);
		
		JSONObject aggObj = new JSONObject();
		aggObj.put("forward", forwardObj);
		aggObj.put("backward", backwardObj);
		
		return aggObj;
	}
	
	public JSONArray getJSONSources() {
		JSONArray sourcesObj = new JSONArray();
		for (CorpNetEdgeSource source : this.sources) {
			sourcesObj.add(source.toJSONObject());
		}
		return sourcesObj;
	}
	
	public String getNet() {
		return this.net;
	}
	
	public String getNode1() {
		return this.node1;
	}
	
	public String getNode2() {
		return this.node2;
	}
	
	public Map<String, Double> getForwardP() {
		return this.forwardP;
	}
	
	public Map<String, Double> getBackwardP() {
		return this.backwardP;
	}
	
	public int getForwardCount() {
		return this.forwardCount;
	}
	
	public int getBackwardCount() {
		return this.backwardCount;
	}
	
	public String getAggregateType() {
		Map<String, Double> aggP = new TreeMap<String, Double>();
		MathUtil.accumulateDistribution(aggP, this.forwardP);
		MathUtil.accumulateDistribution(aggP, this.backwardP);
		return MathUtil.argMaxDistribution(aggP);
	}
	
	public void accumulateForwardP(String key, double p) {
		if (!this.forwardP.containsKey(key))
			this.forwardP.put(key, 0.0);
		this.forwardP.put(key, this.forwardP.get(key) + p);
	}
	
	public void accumulateBackwardP(String key, double p) {
		if (!this.backwardP.containsKey(key))
			this.backwardP.put(key, 0.0);
		this.backwardP.put(key, this.backwardP.get(key) + p);
	}
	
	public void accumulateForwardP(Map<String, Double> p) {
		MathUtil.accumulateDistribution(this.forwardP, p);
	}
	
	public void accumulateBackwardP(Map<String, Double> p) {
		MathUtil.accumulateDistribution(this.backwardP, p);
	}
	
	public void incrementForwardCount() {
		incrementForwardCount(1);
	}
	
	public void incrementForwardCount(int amount) {
		this.forwardCount += amount;
	}
	
	public void incrementBackwardCount() {
		incrementBackwardCount(1);
	}
	
	public void incrementBackwardCount(int amount) {
		this.backwardCount += amount;
	}
	
	public void addSource(CorpNetEdgeSource source) {
		this.sources.add(source);
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		str = str.append(this.net).append(".")
				 .append(this.node1).append(".")
				 .append(this.node2).append("\t")
				 .append(getJSONAggregate().toString()).append("\t")
				 .append(getJSONSources().toString());
		
		return str.toString();
	}
	
	public static CorpNetEdge fromString(String str) {
		return fromString(str, false);
	}
	
	public static CorpNetEdge fromString(String str, boolean ignoreSources) {
		String[] strParts = str.split("\t");
		String keyStr = strParts[0];
		String aggStr = strParts[1];
		String keyParts[] = keyStr.split("\\.");
		String net = keyParts[0];
		String node1 = keyParts[2];
		String node2 = keyParts[3];
		
		CorpNetEdge edge = new CorpNetEdge(net, node1, node2);
		
		JSONObject aggObj = JSONObject.fromObject(aggStr);
		edge.forwardP = JSONUtil.objToDistribution(aggObj.getJSONObject("forwardP"));
		edge.backwardP = JSONUtil.objToDistribution(aggObj.getJSONObject("backwardP"));
		edge.forwardCount = aggObj.getInt("forwardCount");
		edge.backwardCount = aggObj.getInt("backwardCount");
		
		if (strParts.length > 2 && !ignoreSources) {
			JSONArray sourcesObj = JSONArray.fromObject(strParts[2]);
			for (int i = 0; i < sourcesObj.size(); i++)
				edge.addSource(new CorpNetEdgeSource(sourcesObj.getJSONObject(i)));
		}
		
		return edge;
	}
	
	@Override
	public Type getType() {
		return CorpNetObject.Type.EDGE;
	}
}
