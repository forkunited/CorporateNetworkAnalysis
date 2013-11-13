package corp.net;

public abstract class CorpNetObject {
	public enum Type {
		NODE,
		EDGE,
		DOC
	}
	
	protected String net;
	
	public String getNet() {
		return this.net;
	}
	
	public abstract Type getType();
	
	public static CorpNetObject.Type getSerializedObjectType(String str) {
		String[] strParts = str.split("\t");
		String key = strParts[0];
		String[] keyParts = key.split("\\.");
	
		return CorpNetObject.Type.valueOf(keyParts[1]);
	}
	
	public static CorpNetObject fromString(String str) {
		CorpNetObject.Type type = getSerializedObjectType(str);
		if (type.equals(CorpNetObject.Type.DOC)) {
			return CorpNetDoc.fromString(str);
		} else if (type.equals(CorpNetObject.Type.EDGE)) {
			return CorpNetEdge.fromString(str);
		} else if (type.equals(CorpNetObject.Type.NODE)) {
			return CorpNetNode.fromString(str);
		} 
		
		return null;
	}
	
}
