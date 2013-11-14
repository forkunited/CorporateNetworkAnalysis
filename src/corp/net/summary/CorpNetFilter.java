package corp.net.summary;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

public abstract class CorpNetFilter {
	public boolean filterObject(CorpNetObject obj) {
		if (obj.getType().equals(CorpNetObject.Type.DOC)) {
			return filterDoc((CorpNetDoc)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.EDGE)) {
			return filterEdge((CorpNetEdge)obj);
		} else if (obj.getType().equals(CorpNetObject.Type.NODE)) {
			return filterNode((CorpNetNode)obj);
		}
		
		return false;
	}
	
	public abstract boolean filterDoc(CorpNetDoc doc);
	public abstract boolean filterEdge(CorpNetEdge edge);
	public abstract boolean filterNode(CorpNetNode node);
	public abstract String toString();
}

