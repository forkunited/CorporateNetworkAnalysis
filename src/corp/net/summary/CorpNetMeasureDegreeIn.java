package corp.net.summary;

import java.util.HashMap;
import java.util.Map;

import corp.net.CorpNetEdge;
import corp.net.util.MathUtil;

public class CorpNetMeasureDegreeIn extends CorpNetMeasureDegree {
	public CorpNetMeasureDegreeIn() {
		
	}
	
	@Override
	public Map<String, Double> edgeMap(CorpNetEdge edge) {
		Map<String, Double> values = new HashMap<String, Double>(2);
		if (edge.getForwardCount() > 0) {
			String edgeType = MathUtil.argMaxDistribution(edge.getForwardP());
			values.put("NODE/ALL/" + edge.getNode2(), 1.0);
			values.put("NODE/" + edgeType + "/" + edge.getNode2(), 1.0);
		}
		
		if (edge.getBackwardCount() > 0) {
			String edgeType = MathUtil.argMaxDistribution(edge.getBackwardP());
			values.put("NODE/ALL/" + edge.getNode1(), 1.0);
			values.put("NODE/" + edgeType + "/" + edge.getNode1(), 1.0);
		}
		
		return values;
	}

	@Override
	public String getName() {
		return "DEGREE_IN";
	}

}
