package corp.net.summary;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;

public abstract class CorpNetMeasureDegree extends CorpNetMeasure {

	@Override
	public Map<String, Double> map(CorpNetEdge edge) {
		if (edge.getNode1().equals(edge.getNode2()))
			return null;
		
		return edgeMap(edge);
	}
	
	@Override
	public Map<String, Double> map(CorpNetDoc doc) {
		return null;
	}

	@Override
	public Map<String, Double> map(CorpNetNode node) {
		Map<String, Double> values = new HashMap<String, Double>(node.getInP().size() + 1);
		for (String edgeType : node.getInP().keySet())
			values.put(edgeType + "_" + node.getNode(), -1.0);
		values.put("ALL_" + node.getNode(), -1.0);
		return values;
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
	
	protected abstract Map<String, Double> edgeMap(CorpNetEdge edge);
}
