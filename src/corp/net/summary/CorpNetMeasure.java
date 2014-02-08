package corp.net.summary;

import java.util.List;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

/**
 * 
 * Extensions of CorpNetMeasure represent summary measures for the 
 * corporate network (currently used by 
 * corp.net.hadoop.HSummarizeCorpNet).  A measure takes network
 * objects (node, edge, or document), and returns CorpNetSummaryEntry 
 * keys mapped to values.  All currently implemented measures return
 * an entry with a value on each network object for each measure
 * sub-type.
 * 
 * @author Bill McDowell
 *
 */
public abstract class CorpNetMeasure {
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetObject obj) {
		if (obj.getType().equals(CorpNetObject.Type.DOC)) {
			return map(sourceEntry, (CorpNetDoc)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.EDGE)) {
			return map(sourceEntry, (CorpNetEdge)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.NODE)) {
			return map(sourceEntry, (CorpNetNode)obj);
		}
		
		return null;
	}
	
	public abstract List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetEdge edge);
	public abstract List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetDoc doc);
	public abstract List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetNode node);
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
