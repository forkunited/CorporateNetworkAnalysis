package corp.net.summary;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

/**
 * Extensions of CorpNetMeasureDegree compute the number of edges incident to
 * each node that meet certain conditions.  For example, 
 * corp.net.summary.CorpNetMeasureDegreeIn computes the number of edges that
 * are directed toward each node.
 * 
 * @author Bill McDowell
 *
 */
public abstract class CorpNetMeasureDegree extends CorpNetMeasure {

	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetEdge edge) {
		if (edge.getNode1().equals(edge.getNode2()))
			return null;
		
		return edgeMap(sourceEntry, edge);
	}
	
	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetDoc doc) {
		return null;
	}

	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetNode node) {
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>(node.getInP().size() + 1);
		for (String edgeType : node.getInP().keySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType(edgeType);
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(-1.0);
			entries.add(entry);
		}
		
		CorpNetSummaryEntry entry = sourceEntry.clone();
		entry.setMeasureSubType("ALL");
		entry.setObjectType(CorpNetObject.Type.NODE);
		entry.setValue(-1.0);
		entry.setObjectId(node.getNode());
		entries.add(entry);
		
		return entries;
	}

	@Override
	public Double reduce(Iterable<DoubleWritable> values) {
		double sum = 0.0;
		boolean hasNode = false; // Node must be unfiltered (indicated by negative value)
		for (DoubleWritable value : values) {
			if (value.get() < 0)
				hasNode = true;
			else
				sum += value.get();
		}
		
		if (hasNode)
			return sum;
		else
			return null;
	}
	
	protected abstract List<CorpNetSummaryEntry> edgeMap(CorpNetSummaryEntry sourceEntry, CorpNetEdge edge);
}
