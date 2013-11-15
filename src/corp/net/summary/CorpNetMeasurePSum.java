package corp.net.summary;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;

public class CorpNetMeasurePSum  extends CorpNetMeasure {
	public CorpNetMeasurePSum() {
		
	}
	
	@Override
	public Map<String, Double> map(CorpNetEdge edge) {
		return null;
	}

	@Override
	public Map<String, Double> map(CorpNetDoc doc) {
		Map<String, Double> p = doc.getP();
		Map<String, Double> values = new HashMap<String, Double>(p.size());
		for (Entry<String, Double> entry : p.entrySet()) {
			values.put("DOC/" + entry.getKey() + "/" + doc.getDocument(), (double)entry.getValue());
		}
		return values;
	}

	@Override
	public Map<String, Double> map(CorpNetNode node) {
		Map<String, Double> values = new HashMap<String, Double>();
		
		Map<String, Double> inP = node.getInP();
		for (Entry<String, Double> entry : inP.entrySet()) {
			values.put("NODE/IN/" + entry.getKey() + "/" + node.getNode(), (double)entry.getValue());
		}
		
		Map<String, Double> outP = node.getOutP();
		for (Entry<String, Double> entry : outP.entrySet()) {
			values.put("NODE/OUT/" + entry.getKey() + "/" + node.getNode(), (double)entry.getValue());
		}
		
		Map<String, Double> selfP = node.getSelfP();
		for (Entry<String, Double> entry : selfP.entrySet()) {
			values.put("NODE/SELF/" + entry.getKey() + "/" + node.getNode(), (double)entry.getValue());
		}
		
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
		return "P_SUM";
	}
}

