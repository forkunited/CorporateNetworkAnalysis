package corp.net.util;

import ark.util.ARKProperties;

public class CorpNetProperties extends ARKProperties {
	private String networkSourceDirPath;
	private String networkDirPath;
	
	public CorpNetProperties() {
		// FIXME: Do this differently... environment variables...?
		super(new String[] { "corpnet.properties", "/user/wmcdowell/sloan/Projects/CorporateNetworkAnalysis/corpnet.properties" } );

		this.networkSourceDirPath = loadProperty("networkSourceDirPath");
		this.networkDirPath = loadProperty("networkDirPath");
	}
	
	public String getNetworkSourceDirPath() {
		return this.networkSourceDirPath;
	}
	
	public String getNetworkDirPath() {
		return this.networkDirPath;
	}
}
