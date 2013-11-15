package corp.net.summary;

import java.util.ArrayList;
import java.util.List;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.CorpNetObject;

public class CorpNetFilterConjunction extends CorpNetFilter {
	private List<CorpNetFilter> filters;
	
	public CorpNetFilterConjunction(List<CorpNetFilter> filters) {
		this.filters = filters;
	}
	
	public CorpNetFilterConjunction(CorpNetFilter filter1, CorpNetFilter filter2) {
		this.filters = new ArrayList<CorpNetFilter>(2);
		this.filters.add(filter1);
		this.filters.add(filter2);
	}

	@Override
	public boolean filterDoc(CorpNetDoc doc) {
		return conjunctionFilter(doc);
	}

	@Override
	public boolean filterEdge(CorpNetEdge edge) {
		return conjunctionFilter(edge);
	}

	@Override
	public boolean filterNode(CorpNetNode node) {
		return conjunctionFilter(node);
	}
	
	private boolean conjunctionFilter(CorpNetObject netObj) {
		boolean pass = true;
		for (CorpNetFilter filter : this.filters)
			if (!filter.filterObject(netObj))
				pass = false;
		return pass;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str = str.append("CONJUNCTION/");
		for (CorpNetFilter filter : this.filters)
			str = str.append(filter.toString()).append("^");
		str = str.delete(str.length()-1, str.length());
		return str.toString();
	}
}
