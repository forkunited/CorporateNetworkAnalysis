package corp.net.summary;

import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;

public class CorpNetFilterNet extends CorpNetFilter {
	private String net;
	
	public CorpNetFilterNet(String net) {
		this.net = net;
	}
	
	@Override
	public boolean filterDoc(CorpNetDoc doc) {
		return doc.getNet().equals(this.net);
	}

	@Override
	public boolean filterEdge(CorpNetEdge edge) {
		return edge.getNet().equals(this.net);
	}

	@Override
	public boolean filterNode(CorpNetNode node) {
		return node.getNet().equals(this.net);
	}
	
	@Override
	public String toString() {
		return "Net_" + this.net;
	}
}
