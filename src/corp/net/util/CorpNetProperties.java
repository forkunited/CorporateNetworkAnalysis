package corp.net.util;

import ark.util.Properties;

/**
 * CorpNetProperties reads in property values from a configuration file. 
 * These are used throughout the rest of the code.
 * 
 * @author Bill McDowell
 *
 */
public class CorpNetProperties extends Properties {
	private String networkSourceDirPath;
	private String networkDirPath;
	private String corpMetaDataPath;
	private String corpMetaDataGazetteerPath;
	private String bloombergMetaDataPath;
	private String bloombergMetaDataGazetteerPath;
	private String nonCorpInitialismGazetteerPath;
	private String bloombergCorpTickerGazetteerPath;
	private String stopWordGazetteerPath;
	
	public CorpNetProperties() {
		// FIXME: Do this differently... environment variables... Or for now just add
		// your Hadoop cluster path to make it work in the bad way.
		super(new String[] { "corpnet.properties", "/user/wmcdowell/sloan/Projects/CorporateNetworkAnalysis/corpnet.properties" } );

		this.networkSourceDirPath = loadProperty("networkSourceDirPath");
		this.networkDirPath = loadProperty("networkDirPath");
		this.corpMetaDataPath = loadProperty("corpMetaDataPath");
		this.corpMetaDataGazetteerPath = loadProperty("corpMetaDataGazetteerPath");
		this.bloombergMetaDataPath = loadProperty("bloombergMetaDataPath");
		this.bloombergMetaDataGazetteerPath = loadProperty("bloombergMetaDataGazetteerPath");
		this.bloombergCorpTickerGazetteerPath = loadProperty("bloombergCorpTickerGazetteerPath");
		this.nonCorpInitialismGazetteerPath = loadProperty("nonCorpInitialismGazetteerPath");
		this.stopWordGazetteerPath = loadProperty("stopWordGazetteerPath");
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
	
	public String getBloombergCorpTickerGazetteerPath() {
		return this.bloombergCorpTickerGazetteerPath;
	}
	
	public String getNonCorpInitialismGazetteerPath() {
		return this.nonCorpInitialismGazetteerPath;
	}
	
	public String getStopWordGazetteerPath() {
		return this.stopWordGazetteerPath;
	}
}
