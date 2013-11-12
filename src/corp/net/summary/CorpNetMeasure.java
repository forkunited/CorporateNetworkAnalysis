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
		} else if (obj.getType().equals(CorpNetObject.Type.SOURCE)) {
			// Nope
		}
		
		return null;
	}
	
	public abstract Map<String, Double> map(CorpNetEdge edge);
	public abstract Map<String, Double> map(CorpNetDoc doc);
	public abstract Map<String, Double> map(CorpNetNode node);
	public abstract double reduce(Iterable<DoubleWritable> values);
	public abstract String getName();
	
	public static CorpNetMeasure fromString(String str) {
		/* FIXME */
		return null;
	}
}
