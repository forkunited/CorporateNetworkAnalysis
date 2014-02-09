package corp.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import corp.data.CorpMetaData;
import corp.net.util.JSONUtil;
import corp.net.util.MathUtil;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * CorpNetNode represents a node in the corporate network. Instantiations of 
 * this class are first constructed by corp.net.hadoop.HConstructCorpNet, 
 * serialized as JSON objects, and then deserialized by other classes that 
 * process the network. The JSON object representing a CorpNetNode has the
 * form:
 *
 * {
 *  "inCount": <Number of incoming mentions>,
 *  "outCount": <Number of (non-self) outgoing mentions>,
 *  "selfCount": <Number of self mentions>,
 *  "inP": {
 *          "<Relationship type>": <Expected number of incoming mentions>,
 *          <...>
 *         },
 *   "outP": {
 *            "<Relationship type>": <Expected number of outgoing mentions>,
 *            <...>
 *           },
 *   "selfP": {
 *             "<Relationship type>": <Expected number of self mentions>,
 *             <...>  
 *            },
 *   "inTypeCounts": {
 *                    "<Relationship type>": <Number of incoming mentions>,
 *                    <...>
 *                   },
 *   "outTypeCounts": {
 *                     "<Relationship type>": <Number of outgoing mentions>,
 *                     <...>    
 *                    },
 *   "selfTypeCounts": {
 *                      "<Relationship type>": <Number of self mentions>,
 *                      <...>
 *                     },
 *   "metaData": {
 *                "countries": ["<Country>", <...>],
 *                "ciks": ["<CIK>", <...>],
 *                "industries": ["<Industry>", <...>],
 *                "sics": ["<SIC>", <...>],
 *                "tickers": ["<Ticker>", <...>],
 *                "types": ["<Type>", <...>]
 *               }
 * }
 * 
 * 'inCount', 'outCount', and 'selfCount' count the number of mentions (not 
 * edges) in, out, and self-referencing the node.  'inP', 'outP', and 'selfP'
 * sum over the posterior distributions over relationship types for these 
 * mentions.  'inTypeCount', 'outTypeCount', and 'selfTypeCount' count the
 * number of mentions in which each relationship type has the maximum 
 * posterior value.  Note that whether a mention is counted toward  'in', 
 * 'out', or 'self' depends on whether the node represents the author or 
 * mentioned organization in the mention, or both.  A node can represent
 * both the author and the mentioned organization if the entity resolution
 * function (see the Sloan tech report) maps both to the same entity. This
 * has nothing to do with the posterior score for the 'Self-reference' label 
 * from the relationship extraction model---a Self-reference relationship type 
 * posterior value is output for all mentions regardless of whether they are 
 * determined to be 'self' by the entity resolution function or not.
 * 
 * Note that each 'mention' in the mention counts consist of all occurrences 
 * of one (cleaned) organization name within a single document--there is a 
 * single mention for each posterior distribution output by 
 * corp.scratch.RunModelTree from the CorporateRelationExtraction project.  
 * See that class for more details.
 * 
 * @author Bill McDowell
 *
 */
public class CorpNetNode extends CorpNetObject {
	private String node;
	
	private String nonNormalizedName;
	
	private Map<String, Double> inP;
	private Map<String, Double> outP;
	private Map<String, Double> selfP;
	private Map<String, Integer> inTypeCounts;
	private Map<String, Integer> outTypeCounts;
	private Map<String, Integer> selfTypeCounts;
	private int inCount;
	private int outCount;
	private int selfCount;
	
	private List<String> metaDataCountries;
	private List<String> metaDataCiks;
	private List<String> metaDataIndustries;
	private List<String> metaDataSics;
	private List<String> metaDataTickers;
	private List<String> metaDataTypes;
	
	public CorpNetNode(String net, String node) {
		this.net = net;
		this.node = node;
		
		this.nonNormalizedName = "";
		
		this.inP = new TreeMap<String, Double>();
		this.outP = new TreeMap<String, Double>();
		this.selfP = new TreeMap<String, Double>();
		this.inTypeCounts = new TreeMap<String, Integer>();
		this.outTypeCounts = new TreeMap<String, Integer>();
		this.selfTypeCounts = new TreeMap<String, Integer>();
		this.inCount = 0;
		this.outCount = 0;
		this.selfCount = 0;
		
		this.metaDataCountries = new ArrayList<String>();
		this.metaDataCiks = new ArrayList<String>();
		this.metaDataIndustries = new ArrayList<String>();
		this.metaDataSics= new ArrayList<String>();
		this.metaDataTickers = new ArrayList<String>();
		this.metaDataTypes = new ArrayList<String>();
	}
	
	public void loadMetaData(CorpMetaData.Attributes metaData) {
		this.metaDataCountries.addAll(metaData.getCountries());
		this.metaDataCiks.addAll(metaData.getCiks());
		this.metaDataIndustries.addAll(metaData.getIndustries());
		this.metaDataSics.addAll(metaData.getSics());
		this.metaDataTickers.addAll(metaData.getTickers());
		this.metaDataTypes.addAll(metaData.getTypes());
	}
	
	public JSONObject getJSONAggregate() {
		JSONObject inPObj = JSONObject.fromObject(this.inP);
		JSONObject outPObj = JSONObject.fromObject(this.outP);
		JSONObject selfPObj = JSONObject.fromObject(this.selfP);
		JSONObject inTypeCountsObj = JSONObject.fromObject(this.inTypeCounts);
		JSONObject outTypeCountsObj = JSONObject.fromObject(this.outTypeCounts);			
		JSONObject selfTypeCountsObj = JSONObject.fromObject(this.selfTypeCounts);
		JSONArray metaDataCountriesObj = new JSONArray();
		JSONArray metaDataCiksObj = new JSONArray();
		JSONArray metaDataIndustriesObj = new JSONArray();
		JSONArray metaDataSicsObj = new JSONArray();
		JSONArray metaDataTickersObj = new JSONArray();
		JSONArray metaDataTypesObj = new JSONArray();
		
		metaDataCountriesObj.addAll(this.metaDataCountries);
		metaDataCiksObj.addAll(this.metaDataCiks);
		metaDataIndustriesObj.addAll(this.metaDataIndustries);
		metaDataSicsObj.addAll(this.metaDataSics);
		metaDataTickersObj.addAll(this.metaDataTickers);
		metaDataTypesObj.addAll(this.metaDataTypes);
		
		JSONObject metaDataObj = new JSONObject();
		metaDataObj.put("countries", metaDataCountriesObj);
		metaDataObj.put("ciks", metaDataCiksObj);
		metaDataObj.put("industries", metaDataIndustriesObj);
		metaDataObj.put("sics", metaDataSicsObj);
		metaDataObj.put("tickers", metaDataTickersObj);
		metaDataObj.put("types", metaDataTypesObj);
		
		JSONObject obj = new JSONObject();
		obj.put("inCount", this.inCount);
		obj.put("outCount", this.outCount);
		obj.put("selfCount", this.selfCount);
		obj.put("inP", inPObj);
		obj.put("outP", outPObj);
		obj.put("selfP", selfPObj);
		obj.put("inTypeCounts", inTypeCountsObj);
		obj.put("outTypeCounts", outTypeCountsObj);
		obj.put("selfTypeCounts", selfTypeCountsObj);
		obj.put("metaData", metaDataObj);
		
		return obj;
	}
	
	public String getNode() {
		return this.node;
	}
	
	public String getNonNormalizedName() {
		return this.nonNormalizedName;
	}
	
	public Map<String, Double> getInP() {
		return this.inP;
	}
	
	public Map<String, Double> getOutP() {
		return this.outP;
	}
	
	public Map<String, Double> getSelfP() {
		return this.selfP;
	}
	
	public Map<String, Integer> getInTypeCounts() {
		return this.inTypeCounts;
	}
	
	public Map<String, Integer> getOutTypeCounts() {
		return this.outTypeCounts;
	}
	
	public Map<String, Integer> getSelfTypeCounts() {
		return this.selfTypeCounts;
	}
	
	public int getInCount() {
		return this.inCount;
	}
	
	public int getOutCount() {
		return this.outCount;
	}
	
	public int getSelfCount() {
		return this.selfCount;
	}
	
	public List<String> getMetaDataCountries() {
		return this.metaDataCountries;
	}
	
	public List<String> getMetaDataCiks() {
		return this.metaDataCiks;
	}
	
	public List<String> getMetaDataIndustries() {
		return this.metaDataIndustries;
	}
	
	public List<String> getMetaDataSics() {
		return this.metaDataSics;
	}
	
	public List<String> getMetaDataTickers() {
		return this.metaDataTickers;
	}
	
	public List<String> getMetaDataTypes() {
		return this.metaDataTypes;
	}
	
	public void setNonNormalizedName(String nonNormalizedName) {
		this.nonNormalizedName = nonNormalizedName;
	}
	
	public void accumulateInP(String key, double p) {
		if (!this.inP.containsKey(key))
			this.inP.put(key, 0.0);
		this.inP.put(key, this.inP.get(key) + p);
	}
	
	public void accumulateOutP(String key, double p) {
		if (!this.outP.containsKey(key))
			this.outP.put(key, 0.0);
		this.outP.put(key, this.outP.get(key) + p);
	}
	
	public void accumulateSelfP(String key, double p) {
		if (!this.selfP.containsKey(key))
			this.selfP.put(key, 0.0);
		this.selfP.put(key, this.selfP.get(key) + p);
	}
	
	public void accumulateInP(Map<String, Double> p) {
		MathUtil.accumulateDistribution(this.inP, p);
	}
	
	public void accumulateOutP(Map<String, Double> p) {
		MathUtil.accumulateDistribution(this.outP, p);
	}
	
	public void accumulateSelfP(Map<String, Double> p) {
		MathUtil.accumulateDistribution(this.selfP, p);
	}
	
	public void accumulateInTypeCounts(String key, int amount) {
		if (!this.inTypeCounts.containsKey(key))
			this.inTypeCounts.put(key, 0);
		this.inTypeCounts.put(key, this.inTypeCounts.get(key) + amount);
	}
	
	public void accumulateOutTypeCounts(String key, int amount) {
		if (!this.outTypeCounts.containsKey(key))
			this.outTypeCounts.put(key, 0);
		this.outTypeCounts.put(key, this.outTypeCounts.get(key) + amount);
	}
	
	public void accumulateSelfTypeCounts(String key, int amount) {
		if (!this.selfTypeCounts.containsKey(key))
			this.selfTypeCounts.put(key, 0);
		this.selfTypeCounts.put(key, this.selfTypeCounts.get(key) + amount);
	}
	
	public void incrementInCount() {
		incrementInCount(1);
	}
	
	public void incrementInCount(int amount) {
		this.inCount += amount;
	}
	
	public void incrementOutCount() {
		incrementOutCount(1);
	}
	
	public void incrementOutCount(int amount) {
		this.outCount += amount;
	}
	
	public void incrementSelfCount() {
		incrementSelfCount(1);
	}
	
	public void incrementSelfCount(int amount) {
		this.selfCount += amount;
	}
	
	public String toString() {
		StringBuilder str = new StringBuilder();
		str = str.append(this.net).append(".NODE.")
				 .append(this.node).append("\t")
				 .append(getJSONAggregate().toString());
		
		return str.toString();
	}
	
	public static CorpNetNode fromString(String str) {
		String[] strParts = str.split("\t");
		String keyStr = strParts[0];
		String aggStr = strParts[1];
		String keyParts[] = keyStr.split("\\.");
		String net = keyParts[0];
		String n = keyParts[2];
		
		CorpNetNode node= new CorpNetNode(net, n);	
		JSONObject aggObj = JSONObject.fromObject(aggStr);
		
		node.inP = JSONUtil.objToDistribution(aggObj.getJSONObject("inP"));
		node.outP = JSONUtil.objToDistribution(aggObj.getJSONObject("outP"));
		node.selfP = JSONUtil.objToDistribution(aggObj.getJSONObject("selfP"));
		node.inTypeCounts = JSONUtil.objToHistogram(aggObj.getJSONObject("inTypeCounts"));
		node.outTypeCounts = JSONUtil.objToHistogram(aggObj.getJSONObject("outTypeCounts"));
		node.selfTypeCounts = JSONUtil.objToHistogram(aggObj.getJSONObject("selfTypeCounts"));
		node.inCount = aggObj.getInt("inCount");
		node.outCount = aggObj.getInt("outCount");
		node.selfCount = aggObj.getInt("selfCount");
		
		node.metaDataCountries = new ArrayList<String>();
		node.metaDataCiks = new ArrayList<String>();
		node.metaDataIndustries = new ArrayList<String>();
		node.metaDataSics= new ArrayList<String>();
		node.metaDataTickers = new ArrayList<String>();
		node.metaDataTypes = new ArrayList<String>();
		
		JSONObject metaDataObj = aggObj.getJSONObject("metaData");
		JSONArray metaDataCountriesObj = metaDataObj.getJSONArray("countries");
		JSONArray metaDataCiksObj = metaDataObj.getJSONArray("ciks");
		JSONArray metaDataIndustriesObj = metaDataObj.getJSONArray("industries");
		JSONArray metaDataSicsObj = metaDataObj.getJSONArray("sics");
		JSONArray metaDataTickersObj = metaDataObj.getJSONArray("tickers");
		JSONArray metaDataTypesObj = metaDataObj.getJSONArray("types");
		
		for (int i = 0; i < metaDataCountriesObj.size(); i++)
			node.metaDataCountries.add(metaDataCountriesObj.getString(i));
		for (int i = 0; i < metaDataCiksObj.size(); i++)
			node.metaDataCiks.add(metaDataCiksObj.getString(i));
		for (int i = 0; i < metaDataIndustriesObj.size(); i++)
			node.metaDataIndustries.add(metaDataIndustriesObj.getString(i));
		for (int i = 0; i < metaDataSicsObj.size(); i++)
			node.metaDataSics.add(metaDataSicsObj.getString(i));
		for (int i = 0; i < metaDataTickersObj.size(); i++)
			node.metaDataTickers.add(metaDataTickersObj.getString(i));
		for (int i = 0; i < metaDataTypesObj.size(); i++)
			node.metaDataTypes.add(metaDataTypesObj.getString(i));
		
		return node;
	}
	
	@Override
	public Type getType() {
		return CorpNetObject.Type.NODE;
	}
}
