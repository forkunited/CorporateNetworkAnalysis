package corp.net.summary;

import java.util.List;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

public abstract class CorpNetFilter {
	public List<String> filterObject(CorpNetObject obj) {
		if (obj.getType().equals(CorpNetObject.Type.DOC)) {
			return filterDoc((CorpNetDoc)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.EDGE)) {
			return filterEdge((CorpNetEdge)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.NODE)) {
			return filterNode((CorpNetNode)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.SOURCE)) {
			// Nope
		}
		
		return null;
	}
	
	public abstract List<String> filterDoc(CorpNetDoc doc);
	public abstract List<String> filterEdge(CorpNetEdge edge);
	public abstract List<String> filterNode(CorpNetNode node);
	public abstract String getName();
}

