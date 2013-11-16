package corp.net.summary;

import java.util.ArrayList;
import java.util.List;
import corp.net.CorpNetEdge;
import corp.net.CorpNetObject;
import corp.net.util.MathUtil;

public class CorpNetMeasureDegreeTotal extends CorpNetMeasureDegree {
	public CorpNetMeasureDegreeTotal() {
		
	}
	
	@Override
	public List<CorpNetSummaryEntry> edgeMap(CorpNetSummaryEntry sourceEntry, CorpNetEdge edge) {
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>(2);
		if (edge.getForwardCount() > 0 || edge.getBackwardCount() > 0) {
			String edgeType = MathUtil.argMaxDistribution(edge.getForwardP());
			
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType(edgeType);
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(edge.getNode2());
			entry.setValue(1.0);
			entries.add(entry);
			
			entry = sourceEntry.clone();
			entry.setMeasureSubType("ALL");
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(edge.getNode2());
			entry.setValue(1.0);
			entries.add(entry);
			
			entry = sourceEntry.clone();
			entry.setMeasureSubType(edgeType);
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(edge.getNode1());
			entry.setValue(1.0);
			entries.add(entry);
			
			entry = sourceEntry.clone();
			entry.setMeasureSubType("ALL");
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(edge.getNode1());
			entry.setValue(1.0);
			entries.add(entry);
		}
		
		return entries;
	}

	@Override
	public String getName() {
		return "DEGREE_TOTAL";
	}
}
