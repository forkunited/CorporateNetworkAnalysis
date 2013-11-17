package corp.net.summary;

import corp.net.CorpNetObject;

public class CorpNetSummaryEntry {
	public enum AggregationType {
		SUM,
		COUNT,
		HISTOGRAM,
		NONE
	}
	
	private String filterStr; // TODO: deserialize filters
	private CorpNetMeasure measure;
	private CorpNetObject.Type objectType;
	private String measureSubType;
	private String objectId;
	private AggregationType aggType;
	private double value;
	
	public CorpNetSummaryEntry(String filterStr, CorpNetMeasure measure) {
		this.filterStr = filterStr;
		this.measure = measure;
		this.objectType = null;
		this.measureSubType = "";
		this.objectId = "";
		this.aggType = AggregationType.NONE;
		this.value = 0.0;
	}
	
	public String getKey() {
		StringBuilder str = new StringBuilder();
		
		if (this.aggType != AggregationType.NONE)
			str = str.append("AGG/").append(this.aggType).append(".");
		str = str.append(this.filterStr).append(".");
		str = str.append(this.measure.getName()).append(".");
		str = str.append(this.objectType).append("/");
		str = str.append(this.measureSubType).append("//");
		str = str.append(this.objectId);
		
		return str.toString();
	}
	
	public String getFilterStr() {
		return this.filterStr;
	}
	
	public CorpNetMeasure getMeasure() {
		return this.measure;
	}
	
	public CorpNetObject.Type getObjectType() {
		return this.objectType;
	}
	
	public String getMeasureSubType() {
		return this.measureSubType;
	}
	
	public String getObjectId() {
		return this.objectId;
	}
	
	public AggregationType getAggType() {
		return this.aggType;
	}
	
	public double getValue() {
		return this.value;
	}
	
	public void setObjectType(CorpNetObject.Type objectType) {
		this.objectType = objectType;
	}
	
	public void setMeasureSubType(String measureSubType) {
		this.measureSubType = measureSubType;
	}
	
	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}
	
	public void setAggType(AggregationType aggType) {
		this.aggType = aggType;
	}
	
	public void setValue(double value) {
		this.value = value;
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		
		str = str.append(getKey());
		str = str.append("\t").append(this.value);
		
		return str.toString();
	}
	
	public CorpNetSummaryEntry clone() {
		CorpNetSummaryEntry c = new CorpNetSummaryEntry(this.filterStr, this.measure);
		c.setAggType(this.aggType);
		c.setObjectType(this.objectType);
		c.setMeasureSubType(this.measureSubType);
		c.setObjectId(this.objectId);
		c.setValue(this.value);
		return c;
	}
	
	public static CorpNetSummaryEntry fromString(String str) {
		String[] strParts = str.split("\t");
		String key = strParts[0];
		double value =(strParts.length > 1) ? Double.valueOf(strParts[1]) : 0.0;
		
		AggregationType aggType = AggregationType.NONE;
		int filterStart = 0;
		if (key.startsWith("AGG/")) {
			filterStart = key.indexOf(".") + 1;
			aggType = AggregationType.valueOf(key.substring("AGG/".length(), filterStart-1));
		}
		
		int filterEnd = key.indexOf(".", filterStart);
		int measureStart = filterEnd + 1;
		int measureEnd = key.indexOf(".", measureStart);
		int objectTypeStart = measureEnd + 1;
		int objectTypeEnd = key.indexOf("/", objectTypeStart);
		int measureSubTypeStart = objectTypeEnd + 1;
		int measureSubTypeEnd = key.indexOf("//", measureSubTypeStart);
		
		String filterStr = key.substring(filterStart, filterEnd);
		CorpNetMeasure measure = CorpNetMeasure.fromString(key.substring(measureStart, measureEnd));
		CorpNetObject.Type objectType = CorpNetObject.Type.valueOf(key.substring(objectTypeStart, objectTypeEnd));
		String measureSubType = key.substring(measureSubTypeStart, measureSubTypeEnd);
		
		String objectId = "";
		if (measureSubTypeEnd < key.length() - 2) {
			objectId = key.substring(measureSubTypeEnd + 2, key.length());
		}
		
		CorpNetSummaryEntry entry = new CorpNetSummaryEntry(filterStr, measure);
		entry.aggType = aggType;
		entry.objectType = objectType;
		entry.measureSubType = measureSubType;
		entry.objectId = objectId;
		entry.value = value;
		
		return entry;
	}
}
