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
 * CorpNetMeasureMentionCount counts the total number mentions--and the number
 * of mentions of each type--in which each node and document in the network is 
 * involved.  For documents, there are measure sub-types for each relationship
 * type.  For nodes, there are measure sub-types for incoming, outgoing, and
 * self-directed mentions of each relationship type.
 * 
 * @author Bill McDowell
 *
 */
public class CorpNetMeasureMentionCount extends CorpNetMeasure {
	public CorpNetMeasureMentionCount() {
		
	}
	
	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry entry, CorpNetEdge edge) {
		return null;
	}

	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetDoc doc) {
		Map<String, Integer> mentionTypes = doc.getTypeCounts();
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>(mentionTypes.size() + 1);
		for (Entry<String, Integer> e : mentionTypes.entrySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType(e.getKey());
			entry.setObjectType(CorpNetObject.Type.DOC);
			entry.setObjectId(doc.getDocument());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		CorpNetSummaryEntry entry = sourceEntry.clone();
		entry.setMeasureSubType("ALL");
		entry.setObjectType(CorpNetObject.Type.DOC);
		entry.setObjectId(doc.getDocument());
		entry.setValue(doc.getMentionCount());
		entries.add(entry);
		
		return entries;
	}

	@Override
	public List<CorpNetSummaryEntry> map(CorpNetSummaryEntry sourceEntry, CorpNetNode node) {
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>();
		
		Map<String, Integer> inTypes = node.getInTypeCounts();
		for (Entry<String, Integer> e : inTypes.entrySet()) {
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType("IN/" + e.getKey());
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		CorpNetSummaryEntry entry = sourceEntry.clone();
		entry.setMeasureSubType("IN/ALL");
		entry.setObjectType(CorpNetObject.Type.NODE);
		entry.setObjectId(node.getNode());
		entry.setValue(node.getInCount());
		entries.add(entry);
		
		Map<String, Integer> outTypes = node.getOutTypeCounts();
		for (Entry<String, Integer> e : outTypes.entrySet()) {
			entry = sourceEntry.clone();
			entry.setMeasureSubType("OUT/" + e.getKey());
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		entry = sourceEntry.clone();
		entry.setMeasureSubType("OUT/ALL");
		entry.setObjectType(CorpNetObject.Type.NODE);
		entry.setObjectId(node.getNode());
		entry.setValue(node.getOutCount());
		entries.add(entry);
		
		Map<String, Integer> selfTypes = node.getSelfTypeCounts();
		for (Entry<String, Integer> e : selfTypes.entrySet()) {
			entry = sourceEntry.clone();
			entry.setMeasureSubType("SELF/" + e.getKey());
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(node.getNode());
			entry.setValue(e.getValue());
			entries.add(entry);
		}
		
		entry = sourceEntry.clone();
		entry.setMeasureSubType("SELF/ALL");
		entry.setObjectType(CorpNetObject.Type.NODE);
		entry.setObjectId(node.getNode());
		entry.setValue(node.getSelfCount());
		entries.add(entry);
		
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
		return "MENTION_COUNT";
	}

}
