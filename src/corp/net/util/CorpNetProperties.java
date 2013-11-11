package corp.net.util;

import ark.util.ARKProperties;

public class CorpNetProperties extends ARKProperties {
	private String networkSourceDirPath;
	private String networkDirPath;
	private String corpMetaDataPath;
	private String corpMetaDataGazetteerPath;
	private String bloombergMetaDataPath;
	private String bloombergMetaDataGazetteerPath;
	
	public CorpNetProperties() {
		// FIXME: Do this differently... environment variables...?
		super(new String[] { "corpnet.properties", "/user/wmcdowell/sloan/Projects/CorporateNetworkAnalysis/corpnet.properties" } );

		this.networkSourceDirPath = loadProperty("networkSourceDirPath");
		this.networkDirPath = loadProperty("networkDirPath");
		this.corpMetaDataPath = loadProperty("corpMetaDataPath");
		this.corpMetaDataGazetteerPath = loadProperty("corpMetaDataGazetteerPath");
		this.bloombergMetaDataPath = loadProperty("bloombergMetaDataPath");
		this.bloombergMetaDataGazetteerPath = loadProperty("bloombergMetaDataGazetteerPath");
	}
	
	public String getNetworkSourceDirPath() {
		return this.networkSourceDirPath;
	}
	
	public String getNetworkDirPath() {
		return this.networkDirPath;
	}
	
	public String getCorpMetaDataPath() {
		return this.corpMetaDataPath;
	}
	
	public String getCorpMetaDataGazetteerPath() {
		return this.corpMetaDataGazetteerPath;
	}
	
	public String getBloombergMetaDataPath() {
		return this.bloombergMetaDataPath;
	}
	
	public String getBloombergMetaDataGazetteerPath() {
		return this.bloombergMetaDataGazetteerPath;
	}
}
