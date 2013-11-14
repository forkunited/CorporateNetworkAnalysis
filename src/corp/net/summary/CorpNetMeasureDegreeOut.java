package corp.net.summary;

import java.util.HashMap;
import java.util.Map;

import corp.net.CorpNetEdge;
import corp.net.util.MathUtil;

public class CorpNetMeasureDegreeOut extends CorpNetMeasureDegree {
	public CorpNetMeasureDegreeOut() {
		
	}
	
	@Override
	public Map<String, Double> edgeMap(CorpNetEdge edge) {
		Map<String, Double> values = new HashMap<String, Double>(2);
		if (edge.getForwardCount() > 0) {
			String edgeType = MathUtil.argMaxDistribution(edge.getForwardP());
			values.put("ALL_" + edge.getNode1(), 1.0);
			values.put(edgeType + "_" + edge.getNode1(), 1.0);
		}
		
		if (edge.getBackwardCount() > 0) {
			String edgeType = MathUtil.argMaxDistribution(edge.getBackwardP());
			values.put("ALL_" + edge.getNode2(), 1.0);
			values.put(edgeType + "_" + edge.getNode2(), 1.0);
		}
		
		return values;
	}

	@Override
	public String getName() {
		return "DegreeOut";
	}
}
