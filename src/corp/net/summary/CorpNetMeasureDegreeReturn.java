package corp.net.summary;

import java.util.HashMap;
import java.util.Map;

import corp.net.CorpNetEdge;
import corp.net.util.MathUtil;

public class CorpNetMeasureDegreeReturn extends CorpNetMeasureDegree {
	public CorpNetMeasureDegreeReturn() {
		
	}
	
	@Override
	public Map<String, Double> edgeMap(CorpNetEdge edge) {
		Map<String, Double> values = new HashMap<String, Double>(2);
		if (edge.getForwardCount() > 0 && edge.getBackwardCount() > 0) {
			
			values.put("ALL_" + edge.getNode1(), 1.0);
			values.put("ALL_" + edge.getNode2(), 1.0);
			
			String forwardEdgeType = MathUtil.argMaxDistribution(edge.getForwardP());
			String backwardEdgeType = MathUtil.argMaxDistribution(edge.getBackwardP());
			if (forwardEdgeType.equals(backwardEdgeType)) {
				values.put(forwardEdgeType + "_" + edge.getNode1(), 1.0);
				values.put(forwardEdgeType + "_" + edge.getNode2(), 1.0);
			}
		}
		
		return values;
	}

	@Override
	public String getName() {
		return "DegreeReturn";
	}
}
