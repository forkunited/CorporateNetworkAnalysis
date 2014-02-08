package corp.net.summary;

import java.util.ArrayList;
import java.util.List;

import corp.net.CorpNetEdge;
import corp.net.CorpNetObject;
import corp.net.util.MathUtil;

/**
 * CorpNetMeasureDegreeReturn computes total number of edges and the number of 
 * edges of each type for which there is a return edge in the opposite 
 * direction between two nodes.  Each relationship type is a measure sub-type.
 * 
 * @author Bill McDowell
 *
 */
public class CorpNetMeasureDegreeReturn extends CorpNetMeasureDegree {
	public CorpNetMeasureDegreeReturn() {
		
	}
	
	@Override
	public List<CorpNetSummaryEntry> edgeMap(CorpNetSummaryEntry sourceEntry, CorpNetEdge edge) {
		List<CorpNetSummaryEntry> entries = new ArrayList<CorpNetSummaryEntry>(2);
		if (edge.getForwardCount() > 0 && edge.getBackwardCount() > 0) {
			
			
			CorpNetSummaryEntry entry = sourceEntry.clone();
			entry.setMeasureSubType("ALL");
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(edge.getNode1());
			entry.setValue(1.0);
			entries.add(entry);
			
			entry = sourceEntry.clone();
			entry.setMeasureSubType("ALL");
			entry.setObjectType(CorpNetObject.Type.NODE);
			entry.setObjectId(edge.getNode2());
			entry.setValue(1.0);
			entries.add(entry);
			
			String forwardEdgeType = MathUtil.argMaxDistribution(edge.getForwardP());
			String backwardEdgeType = MathUtil.argMaxDistribution(edge.getBackwardP());
			if (forwardEdgeType.equals(backwardEdgeType)) {
				entry = sourceEntry.clone();
				entry.setMeasureSubType(forwardEdgeType);
				entry.setObjectType(CorpNetObject.Type.NODE);
				entry.setObjectId(edge.getNode1());
				entry.setValue(1.0);
				entries.add(entry);
				
				entry = sourceEntry.clone();
				entry.setMeasureSubType(forwardEdgeType);
				entry.setObjectType(CorpNetObject.Type.NODE);
				entry.setObjectId(edge.getNode2());
				entry.setValue(1.0);
				entries.add(entry);
			}
		}
		
		return entries;
	}

	@Override
	public String getName() {
		return "DEGREE_RETURN";
	}
}
