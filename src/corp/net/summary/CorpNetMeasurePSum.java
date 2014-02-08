package corp.net.summary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

/**
 * CorpNetMeasurePSum computes the sum of relationship type posteriors across
 * all mentions in which each node and document in the network is involved.   
 * For documents, there are measure sub-types for the sum of each relationship 
 * type.  For nodes, there are measure sub-types for sums that include 
 * incoming, out-going, and self-directed mentions of each relationship type.
 * 
 * @author Bill McDowell
 *
 */
public class CorpNetMeasurePSum  extends CorpNetMeasure {
	public CorpNetMeasurePSum() {
		
	}
	
	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetEdge edge) {
		return null;
	}

	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetDoc doc) {
		Map<String, Double> p = doc.getP();
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>(p.size());
		for (Entry<String, Double> e : p.entrySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType(e.getKey());
			entry.setObjectType(CorpNetObject.Type.DOC);
			entry.setObjectId(doc.getDocument());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		return entries;
	}

	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetNode node) {
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>();
		Map<String, Double> inP = node.getInP();
		for (Entry<String, Double> e : inP.entrySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType("IN/" + e.getKey());
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		Map<String, Double> outP = node.getOutP();
		for (Entry<String, Double> e : outP.entrySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType("OUT/" + e.getKey());
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		Map<String, Double> selfP = node.getSelfP();
		for (Entry<String, Double> e : selfP.entrySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType("SELF/" + e.getKey());
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		return entries;
	}

	@Override
	public Double reduce(Iterable<DoubleWritable> values) {
		double sum = 0.0;
		for (DoubleWritable value : values)
			sum += value.get();
		return sum;
	}

	@Override
	public String getName() {
		return "P_SUM";
	}
}

