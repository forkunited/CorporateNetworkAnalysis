package corp.net;

import java.util.Map;
import java.util.TreeMap;

import corp.net.util.JSONUtil;
import corp.net.util.MathUtil;
import net.sf.json.JSONObject;

public class CorpNetDoc extends CorpNetObject {
	private String document;
	
	private Map<String, Double> p;
	private Map<String, Integer> typeCounts;
	private int mentionCount = 0;
	
	public CorpNetDoc(String net, String document) {
		this.net = net;
		this.document = document;
		this.p = new TreeMap<String, Double>();
		this.typeCounts = new TreeMap<String, Integer>();
		this.mentionCount = 0;
	}
	
	public JSONObject getJSONAggregate() {
		JSONObject aggObj = new JSONObject();
		aggObj.put("typeCounts", JSONObject.fromObject(this.typeCounts));
		aggObj.put("mentionCount", this.mentionCount);
		aggObj.put("p", JSONObject.fromObject(this.p));
		return aggObj;
	}
	
	public String getDocument() {
		return this.document;
	}
	
	public Map<String, Double> getP() {
		return this.p;
	}
	
	public Map<String, Integer> getTypeCounts() {
		return this.typeCounts;
	}
	
	public int getMentionCount() {
		return this.mentionCount;
	}
	
	public void accumulateP(String key, double p) {
		if (!this.p.containsKey(key))
			this.p.put(key, 0.0);
		this.p.put(key, this.p.get(key) + p);
	}
	
	public void accumulateP(Map<String, Double> p) {
		MathUtil.accumulateDistribution(this.p, p);
	}
	
	public void accumulateTypeCounts(String key, int amount) {
		if (!this.typeCounts.containsKey(key))
			this.typeCounts.put(key, 0);
		this.typeCounts.put(key, this.typeCounts.get(key) + amount);
	}
	
	public void incrementMentionCount() {
		incrementMentionCount(1);
	}
	
	public void incrementMentionCount(int amount) {
		this.mentionCount += amount;
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		str = str.append(this.net).append(".DOC.")
				 .append(this.document).append("\t")
				 .append(getJSONAggregate().toString());
		
		return str.toString();
	}
	
	public static CorpNetDoc fromString(String str) {
		String[] strParts = str.split("\t");
		String keyStr = strParts[0];
		String aggStr = strParts[1];
		String keyParts[] = keyStr.split("\\.");
		String net = keyParts[0];
		String d = keyParts[2];
		
		CorpNetDoc doc = new CorpNetDoc(net, d);
		
		JSONObject aggObj = JSONObject.fromObject(aggStr);
		doc.p = JSONUtil.objToDistribution(aggObj.getJSONObject("p"));
		doc.typeCounts = JSONUtil.objToHistogram(aggObj.getJSONObject("typeCounts"));
		doc.mentionCount = aggObj.getInt("mentionCount");
		
		return doc;
	}

	@Override
	public Type getType() {
		return CorpNetObject.Type.DOC;
	}
}
