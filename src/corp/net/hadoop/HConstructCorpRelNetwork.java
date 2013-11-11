package corp.net.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import corp.data.CorpMetaData;
import corp.net.util.CorpNetProperties;
import corp.util.CorpKeyFn;

import ark.data.Gazetteer;
import ark.util.StringUtil;

public class HConstructCorpRelNetwork {
	
	public static class HConstructCorpRelNetworkMapper extends Mapper<Object, Text, Text, Text> {
		private Text netObjId = new Text();
		private Text netObj = new Text();
		
		private CorpNetProperties properties = new CorpNetProperties();
		private Gazetteer stopWordGazetteer = new Gazetteer("StopWord", this.properties.getStopWordGazetteerPath());
		private Gazetteer bloombergCorpTickerGazetteer = new Gazetteer("BloombergCorpTickerGazetteer", this.properties.getBloombergCorpTickerGazetteerPath());
		private StringUtil.StringTransform stopWordCleanFn = StringUtil.getStopWordsCleanFn(this.stopWordGazetteer);
		private CorpKeyFn corpKeyFn = new CorpKeyFn(this.bloombergCorpTickerGazetteer, this.stopWordCleanFn);
	
		/*
		 * Skip badly gzip'd files
		 */
		public void run(Context context) throws InterruptedException {
			try {
				setup(context);
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
				}
				cleanup(context);
			} catch (Exception e) {

			}
		}

		public void map(Object key, Text value, Context context) {
			try {
				JSONObject relationObj = JSONObject.fromObject(value.toString());
				if (relationObj != null) {
					JSONArray mentions = relationObj.getJSONArray("mentions");
					if (mentions.size() == 0) {
						throw new IllegalArgumentException("Relation object missing mentions...");
					}
					
					String mention = this.corpKeyFn.transform(mentions.getJSONObject(0).getString("text"));
					String author = this.corpKeyFn.transform(relationObj.getString("author"));
					String annotationFile = relationObj.getString("annotationFile");
					int dateStartIndex = annotationFile.indexOf("-8-K-") + 5;
					String year = annotationFile.substring(dateStartIndex, dateStartIndex+4);
					
					// Undirected edge id
					String edgeId = null;
					JSONObject edgeValue = new JSONObject();
					edgeValue.put("source", relationObj);
					
					if (mention.compareTo(author) < 0) {
						edgeId = "EDGE." + mention + "." + author;
						edgeValue.put("direction", "BACKWARD");
					} else {
						edgeId = "EDGE." + author + "." + mention;
						edgeValue.put("direction", "FORWARD");
					}
					String yearEdgeId = year + "." + edgeId;
					String fullEdgeId = "FULL." + edgeId;
					
					this.netObj.set(edgeValue.toString());
					this.netObjId.set(yearEdgeId);
					context.write(this.netObjId, this.netObj);
					this.netObjId.set(fullEdgeId);
					context.write(this.netObjId, this.netObj);
					
					// Set output object to author value
					JSONObject authorValue = new JSONObject();
					authorValue.put("source", relationObj);
					authorValue.put("isAuthor", true);
					authorValue.put("isSelf", author.equals(mention));
					this.netObj.set(authorValue.toString());
					
					// Author id
					String authorId = "NODE." + author;
					String yearAuthorId = year + "." + authorId;
					String fullAuthorId = "FULL." + authorId;
					
					this.netObjId.set(yearAuthorId);
					context.write(this.netObjId, this.netObj);
					this.netObjId.set(fullAuthorId);
					context.write(this.netObjId, this.netObj);
					
					// Mention id
					if (!author.equals(mention)) {
						// Set output object to mention value
						JSONObject mentionValue = new JSONObject();
						mentionValue.put("source", relationObj);
						mentionValue.put("isAuthor", false);
						mentionValue.put("isSelf", false);
						this.netObj.set(mentionValue.toString());
						
						String mentionId = "NODE." + mention;
						String yearMentionId = year + "." + mentionId;
						String fullMentionId = "FULL." + mentionId;
						
						this.netObjId.set(yearMentionId);
						context.write(this.netObjId, this.netObj);
						this.netObjId.set(fullMentionId);
						context.write(this.netObjId, this.netObj);
					}
					
					// Setup document id
					this.netObj.set(value);
					String documentId = "DOC." + annotationFile.split("\\.")[0];
					String yearDocumentId = year + "." + documentId;
					String fullDocumentId = "FULL." + documentId;
					
					this.netObjId.set(yearDocumentId);
					context.write(this.netObjId, this.netObj);
					this.netObjId.set(fullDocumentId);
					context.write(this.netObjId, this.netObj);
				} else {
					throw new IllegalArgumentException("Invalid relation object.");
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public static class HConstructCorpRelNetworkReducer extends Reducer<Text, Text, Text, Text> {
		private Text fullText = new Text();
		
		private CorpNetProperties properties = new CorpNetProperties();
		private CorpMetaData corpMetaData = new CorpMetaData("Corp", properties.getCorpMetaDataPath());
		private Gazetteer corpMetaDataGazetteer = new Gazetteer("CorpMetaData", properties.getCorpMetaDataGazetteerPath());
		private CorpMetaData bloombergMetaData = new CorpMetaData("Bloomberg", this.properties.getBloombergMetaDataPath());
		private Gazetteer bloombergMetaDataGazetteer =  new Gazetteer("BloombergMetaData", this.properties.getBloombergMetaDataGazetteerPath());
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			this.fullText.clear();
			String keyStr = key.toString();
			String[] keyParts = keyStr.split("\\.");
			String net = keyParts[0];
			if (keyParts[1].equals("EDGE")) {
				String n1 = keyParts[2];
				String n2 = keyParts[3];
				reduceEdge(key, net, n1, n2, values, context);
			} else if (keyParts[1].equals("NODE")) {
				String n = keyParts[2];
				reduceNode(key, net, n, values, context);
			} else if (keyParts[1].equals("DOC")) {
				String doc = keyParts[2];
				reduceDoc(key, net, doc, values, context);
			}
		}
			
		private void reduceEdge(Text key, String net, String n1, String n2, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Double> forwardP = new TreeMap<String, Double>();
			Map<String, Double> backwardP = new TreeMap<String, Double>();
			int forwardCount = 0, backwardCount = 0;
			JSONArray sources = new JSONArray();
			
			for (Text text : values) {
				JSONObject edgeObj = JSONObject.fromObject(text.toString());
				SourceObject srcObj = new SourceObject(edgeObj.getJSONObject("source"));
				String direction = edgeObj.getString("direction");
				
				sources.add(edgeObj);
				Map<String, Double> p = srcObj.getP();
				Map<String, Double> aggP = null;
				if (direction.equals("FORWARD")) {
					aggP = forwardP;
					forwardCount++;
				} else {
					aggP = backwardP;
					backwardCount++;
				}
				
				for (Entry<String, Double> pEntry : p.entrySet()) {
					if (!aggP.containsKey(pEntry.getKey()))
						aggP.put(pEntry.getKey(), 0.0);
					aggP.put(pEntry.getKey(), aggP.get(pEntry.getKey()) + pEntry.getValue());
				}
			}
			
			JSONObject forwardPObj = JSONObject.fromObject(forwardP);
			JSONObject forwardObj = new JSONObject();
			forwardObj.put("p", forwardPObj);
			forwardObj.put("count", forwardCount);
			
			JSONObject backwardPObj = JSONObject.fromObject(backwardP);
			JSONObject backwardObj = new JSONObject();
			backwardObj.put("p", backwardPObj);
			backwardObj.put("count", backwardCount);
			
			JSONObject aggObj = new JSONObject();
			aggObj.put("forward", forwardObj);
			aggObj.put("backward", backwardObj);
			
			byte[] aggregateStr = aggObj.toString().getBytes();
			byte[] sourcesStr = sources.toString().getBytes();
			
			this.fullText.append(aggregateStr, 0, aggregateStr.length);
			this.fullText.append("\t".getBytes(), 0, 1);
			this.fullText.append(sourcesStr, 0, sourcesStr.length);
			
			context.write(key, this.fullText);
		}
		
		private void reduceDoc(Text key, String net, String n, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			Map<String, Double> aggP = new TreeMap<String, Double>();
			Map<String, Integer> mentionTypeCounts = new TreeMap<String, Integer>();
			int mentionCount = 0;
			
			for (Text text : values) {
				SourceObject srcObj = new SourceObject(text);
				String mentionType = srcObj.getMaxType();
				
				Map<String, Double> p  = srcObj.getP();
				for (Entry<String, Double> entry : p.entrySet()) {
					if (!aggP.containsKey(entry.getKey())) {
						aggP.put(entry.getKey(), 0.0);
						mentionTypeCounts.put(entry.getKey(), 0);
					}
					
					aggP.put(entry.getKey(), aggP.get(entry.getKey()) + entry.getValue());
				}
				
				mentionTypeCounts.put(mentionType, mentionTypeCounts.get(mentionType) + 1);
				
				mentionCount++;
			}
			
			JSONObject aggObj = new JSONObject();
			JSONObject mentionTypeCountsObj = JSONObject.fromObject(mentionTypeCounts);
			JSONObject pObj = JSONObject.fromObject(aggP);
			
			aggObj.put("typeCounts", mentionTypeCountsObj);
			aggObj.put("count",  mentionCount);
			aggObj.put("p", pObj);
			
			byte[] aggStr = aggObj.toString().getBytes();
			this.fullText.append(aggStr, 0, aggStr.length);
			context.write(key, this.fullText);
		}
		
		private void reduceNode(Text key, String net, String n, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
			
			Map<String, Double> inP = new TreeMap<String, Double>();
			Map<String, Double> outP = new TreeMap<String, Double>();
			Map<String, Integer> inTypeCounts = new TreeMap<String, Integer>();
			Map<String, Integer> outTypeCounts = new TreeMap<String, Integer>();
			int inCount = 0;
			int outCount = 0;
			int selfCount = 0;
			JSONObject metaDataObj = null;
			
			for (Text value : values) {
				JSONObject nodeObj = JSONObject.fromObject(value.toString());
				SourceObject srcObj = new SourceObject(nodeObj.getJSONObject("source"));
				
				if (metaDataObj == null) {
					if (nodeObj.getBoolean("isAuthor"))
						metaDataObj = getNodeMetaData(srcObj.getNonnormalizedAuthor());
					else
						metaDataObj = getNodeMetaData(srcObj.getNonnormalizedMention());
				}
				
				if (nodeObj.getBoolean("isSelf")) {
					selfCount++;
				} else if (nodeObj.getBoolean("isAuthor")) {
					Map<String, Double> p = srcObj.getP();
					for (Entry<String, Double> pEntry : p.entrySet()) {
						if (!outP.containsKey(pEntry.getKey())) {
							outP.put(pEntry.getKey(), 0.0);
							outTypeCounts.put(pEntry.getKey(), 0);
						}
						outP.put(pEntry.getKey(), outP.get(pEntry.getKey()) + pEntry.getValue());
					}
					String maxType = srcObj.getMaxType();
					outTypeCounts.put(maxType, outTypeCounts.get(maxType) + 1);
					
					outCount++;
				} else {
					Map<String, Double> p = srcObj.getP();
					for (Entry<String, Double> pEntry : p.entrySet()) {
						if (!inP.containsKey(pEntry.getKey())) {
							inP.put(pEntry.getKey(), 0.0);
							inTypeCounts.put(pEntry.getKey(), 0);
						}
						inP.put(pEntry.getKey(), inP.get(pEntry.getKey()) + pEntry.getValue());
					}
					String maxType = srcObj.getMaxType();
					inTypeCounts.put(maxType, inTypeCounts.get(maxType) + 1);
					
					inCount++;
				}
			}
			
			JSONObject aggObj = new JSONObject();
			JSONObject inPObj = JSONObject.fromObject(inP);
			JSONObject outPObj = JSONObject.fromObject(outP);
			JSONObject inTypeCountsObj = JSONObject.fromObject(inTypeCounts);
			JSONObject outTypeCountsObj = JSONObject.fromObject(outTypeCounts);			
			
			aggObj.put("inCount", inCount);
			aggObj.put("outCount", outCount);
			aggObj.put("selfCount", selfCount);
			aggObj.put("inPObj", inPObj);
			aggObj.put("outPObj", outPObj);
			aggObj.put("inTypeCountsObj", inTypeCountsObj);
			aggObj.put("outTypeCountsObj", outTypeCountsObj);
			aggObj.put("metaData", metaDataObj);
			
			byte[] aggStr = aggObj.toString().getBytes();
			this.fullText.append(aggStr, 0, aggStr.length);
			context.write(key, this.fullText);
		}
		
		private JSONObject getNodeMetaData(String node) {
			JSONObject attsObj = new JSONObject();
			attsObj.put("countries", new JSONArray());
			attsObj.put("ciks", new JSONArray());
			attsObj.put("industries", new JSONArray());
			attsObj.put("sics", new JSONArray());
			attsObj.put("tickers", new JSONArray());
			attsObj.put("types", new JSONArray());

			List<String> ids = this.corpMetaDataGazetteer.getIds(node);
			if (ids != null) {
				for (String id : ids) {
					CorpMetaData.Attributes atts = this.corpMetaData.getAttributesById(id);
					attsObj.getJSONArray("countries").addAll(atts.getCountries());
					attsObj.getJSONArray("ciks").addAll(atts.getCiks());
					attsObj.getJSONArray("industries").addAll(atts.getIndustries());
					attsObj.getJSONArray("sics").addAll(atts.getSics());
					attsObj.getJSONArray("tickers").addAll(atts.getTickers());
					attsObj.getJSONArray("types").addAll(atts.getTypes());
				}
			}

			ids = this.bloombergMetaDataGazetteer.getIds(node);
			if (ids != null) {
				for (String id : ids) {
					CorpMetaData.Attributes atts = this.bloombergMetaData.getAttributesById(id);
					attsObj.getJSONArray("countries").addAll(atts.getCountries());
					attsObj.getJSONArray("ciks").addAll(atts.getCiks());
					attsObj.getJSONArray("industries").addAll(atts.getIndustries());
					attsObj.getJSONArray("sics").addAll(atts.getSics());
					attsObj.getJSONArray("tickers").addAll(atts.getTickers());
					attsObj.getJSONArray("types").addAll(atts.getTypes());
				}		
			}
			
			return attsObj;
		}
		
		private static class SourceObject {
			private Map<String, Double> p;
			private String nonnormalizedAuthor;
			private String firstNonnormalizedMention;
			
			public SourceObject(Text text) {
				this(text.toString());
			}
			
			public SourceObject(String text) {
				this(JSONObject.fromObject(text));
			}
			
			public SourceObject(JSONObject textJSON) {
				this.firstNonnormalizedMention = textJSON.getJSONArray("mentions").getJSONObject(0).getString("text");
				this.nonnormalizedAuthor = textJSON.getString("author");
				this.p = new TreeMap<String, Double>();
				JSONObject pObj = textJSON.getJSONObject("p");
				Set pEntries = pObj.entrySet();
				for (Object o : pEntries) {
					Entry e = (Entry)o;
					String label = e.getKey().toString();
					double pValue = Double.parseDouble(e.getValue().toString());
					this.p.put(label, pValue);
				}
				
			}
			
			public Map<String, Double> getP() {
				return this.p;
			}
			
			public String getNonnormalizedAuthor() {
				return this.nonnormalizedAuthor;
			}
			
			public String getNonnormalizedMention() {
				return this.firstNonnormalizedMention;
			}
			
			public String getMaxType() {
				String maxType = null;
				double max = 0;
				for (Entry<String, Double> entry : this.p.entrySet()) {
					if (entry.getValue() > max) {
						max = entry.getValue();
						maxType = entry.getKey();
					}
				}
				return maxType;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HConstructCorpRelNetwork");
		job.setJarByClass(HConstructCorpRelNetwork.class);
		job.setMapperClass(HConstructCorpRelNetworkMapper.class);
		job.setReducerClass(HConstructCorpRelNetworkReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


