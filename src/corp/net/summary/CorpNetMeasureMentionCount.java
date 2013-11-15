package corp.net.summary;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;

public class CorpNetMeasureMentionCount extends CorpNetMeasure {
	public CorpNetMeasureMentionCount() {
		
	}
	
	@Override
	public Map<String, Double> map(CorpNetEdge edge) {
		return null;
	}

	@Override
	public Map<String, Double> map(CorpNetDoc doc) {
		Map<String, Integer> mentionTypes = doc.getTypeCounts();
		Map<String, Double> values = new HashMap<String, Double>(mentionTypes.size() + 1);
		for (Entry<String, Integer> entry : mentionTypes.entrySet()) {
			values.put(entry.getKey() + "/" + doc.getDocument(), (double)entry.getValue());
		}
		values.put("ALL/" + doc.getDocument(), (double)doc.getMentionCount());
		return values;
	}

	@Override
	public Map<String, Double> map(CorpNetNode node) {
		Map<String, Double> values = new HashMap<String, Double>();
		
		Map<String, Integer> inTypes = node.getInTypeCounts();
		for (Entry<String, Integer> entry : inTypes.entrySet()) {
			values.put("IN/" + entry.getKey() + "/" + node.getNode(), (double)entry.getValue());
		}
		values.put("IN/ALL/" + node.getNode(), (double)node.getInCount());
		
		Map<String, Integer> outTypes = node.getOutTypeCounts();
		for (Entry<String, Integer> entry : outTypes.entrySet()) {
			values.put("OUT/" + entry.getKey() + "/" + node.getNode(), (double)entry.getValue());
		}
		values.put("OUT/ALL/" + node.getNode(), (double)node.getOutCount());
		
		Map<String, Integer> selfTypes = node.getSelfTypeCounts();
		for (Entry<String, Integer> entry : selfTypes.entrySet()) {
			values.put("SELF/" + entry.getKey() + "/" + node.getNode(), (double)entry.getValue());
		}
		values.put("SELF/ALL/" + node.getNode(), (double)node.getSelfCount());
		
		return values;
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
