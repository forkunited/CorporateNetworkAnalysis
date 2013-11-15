package corp.net.summary;

import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

public abstract class CorpNetMeasure {
	public Map<String, Double> map(CorpNetObject obj) {
		if (obj.getType().equals(CorpNetObject.Type.DOC)) {
			return map((CorpNetDoc)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.EDGE)) {
			return map((CorpNetEdge)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.NODE)) {
			return map((CorpNetNode)obj);
		}
		
		return null;
	}
	
	public abstract Map<String, Double> map(CorpNetEdge edge);
	public abstract Map<String, Double> map(CorpNetDoc doc);
	public abstract Map<String, Double> map(CorpNetNode node);
	public abstract Double reduce(Iterable<DoubleWritable> values);
	public abstract String getName();
	
	public static CorpNetMeasure fromString(String str) {
		if (str.equals("DEGREE_IN")) {
			return new CorpNetMeasureDegreeIn();
		} else if (str.equals("DEGREE_OUT")) {
			return new CorpNetMeasureDegreeOut();
		} else if (str.equals("DEGREE_RETURN")) {
			return new CorpNetMeasureDegreeReturn();
		} else if (str.equals("DEGREE_TOTAL")) {
			return new CorpNetMeasureDegreeTotal();
		} else if (str.equals("MENTION_COUNT")) {
			return new CorpNetMeasureMentionCount();
		} else if (str.equals("P_SUM")) {
			return new CorpNetMeasurePSum();
		} else {
			return null;
		}
	}
}
