package corp.net.summary;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;

public class CorpNetFilterNodeSic extends CorpNetFilter {
	private String sic;
	
	public CorpNetFilterNodeSic(String sic) {
		this.sic = sic;
	}
	
	@Override
	public boolean filterDoc(CorpNetDoc doc) {
		return true;
	}

	@Override
	public boolean filterEdge(CorpNetEdge edge) {
		return true;
	}

	@Override
	public boolean filterNode(CorpNetNode node) {
		return node.getMetaDataSics().contains(this.sic);
	}

	@Override
	public String toString() {
		return "NODE_SIC/" + this.sic;
	}

}
