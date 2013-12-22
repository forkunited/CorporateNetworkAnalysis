package corp.net.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import corp.net.CorpNetDoc;
import corp.net.CorpNetEdge;
import corp.net.CorpNetEdgeSource;
import corp.net.CorpNetNode;
import corp.net.util.CorpNetProperties;
import corp.util.CorpKeyFn;

import ark.data.Gazetteer;
import ark.util.StringUtil;

public class HConstructCorpNet {
	
	public static class HConstructCorpNetMapper extends Mapper<Object, Text, Text, Text> {
		private Text netObjId = new Text();
		private Text netObj = new Text();
		
		private CorpNetProperties properties = new CorpNetProperties();
		private Gazetteer stopWordGazetteer = new Gazetteer("StopWord", this.properties.getStopWordGazetteerPath());
		private Gazetteer bloombergCorpTickerGazetteer = new Gazetteer("BloombergCorpTickerGazetteer", this.properties.getBloombergCorpTickerGazetteerPath());
		private Gazetteer nonCorpInitialismGazetteer = new Gazetteer("NonCorpInitialismGazetteer", this.properties.getNonCorpInitialismGazetteerPath());
		private StringUtil.StringTransform stopWordCleanFn = StringUtil.getStopWordsCleanFn(this.stopWordGazetteer);
		private CorpKeyFn corpKeyFn = null; 
	

		public void run(Context context) throws InterruptedException, IOException {
			List<Gazetteer> corpKeyFnKeyMaps = new ArrayList<Gazetteer>();
			corpKeyFnKeyMaps.add(this.bloombergCorpTickerGazetteer);
			corpKeyFnKeyMaps.add(this.nonCorpInitialismGazetteer);
			this.corpKeyFn = new CorpKeyFn(corpKeyFnKeyMaps, this.stopWordCleanFn);
			
			setup(context);
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(),
						context);
			}
			cleanup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
				
				if (mention.trim().length() == 0 || author.trim().length() == 0)
					return;
				
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
		}
	}

	public static class HConstructCorpNetReducer extends Reducer<Text, Text, Text, Text> {
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
			CorpNetEdge edge = new CorpNetEdge(net, n1, n2);
			for (Text text : values) {
				JSONObject edgeObj = JSONObject.fromObject(text.toString());
				CorpNetEdgeSource srcObj = new CorpNetEdgeSource(edgeObj.getJSONObject("source"));
				String direction = edgeObj.getString("direction");
				edge.addSource(srcObj);
				
				if (direction.equals("FORWARD")) {
					edge.accumulateForwardP(srcObj.getP());
					edge.incrementForwardCount();
				} else {
					edge.accumulateBackwardP(srcObj.getP());
					edge.incrementBackwardCount();
				}
			}
			
			byte[] aggregateStr = edge.getJSONAggregate().toString().getBytes();
			byte[] sourcesStr = edge.getJSONSources().toString().getBytes();
			
			this.fullText.append(aggregateStr, 0, aggregateStr.length);
			this.fullText.append("\t".getBytes(), 0, 1);
			this.fullText.append(sourcesStr, 0, sourcesStr.length);
			
			context.write(key, this.fullText);
		}
		
		private void reduceDoc(Text key, String net, String d, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			CorpNetDoc doc = new CorpNetDoc(net, d);
			for (Text text : values) {
				CorpNetEdgeSource srcObj = new CorpNetEdgeSource(text);
				doc.accumulateP(srcObj.getP());
				doc.incrementMentionCount();
				doc.accumulateTypeCounts(srcObj.getMaxType(), 1);
			}
			
			byte[] aggStr = doc.getJSONAggregate().toString().getBytes();
			this.fullText.append(aggStr, 0, aggStr.length);
			context.write(key, this.fullText);
		}
		
		private void reduceNode(Text key, String net, String n, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
			CorpNetNode node = new CorpNetNode(net, n);
			for (Text value : values) {
				JSONObject nodeObj = JSONObject.fromObject(value.toString());
				CorpNetEdgeSource srcObj = new CorpNetEdgeSource(nodeObj.getJSONObject("source"));
				
				if (node.getNonNormalizedName().length() == 0) {
					if (nodeObj.getBoolean("isAuthor"))
						node.setNonNormalizedName(srcObj.getAuthor());
					else
						node.setNonNormalizedName(srcObj.getMentions().get(0));
					
					List<String> ids = this.corpMetaDataGazetteer.getIds(node.getNonNormalizedName());
					if (ids != null) {
						for (String id : ids) {
							node.loadMetaData(this.corpMetaData.getAttributesById(id));
						}
					}

					ids = this.bloombergMetaDataGazetteer.getIds(node.getNonNormalizedName());
					if (ids != null) {
						for (String id : ids) {
							node.loadMetaData(this.bloombergMetaData.getAttributesById(id));
						}
					}
				}
				
				if (nodeObj.getBoolean("isSelf")) {
					node.incrementSelfCount();
					node.accumulateSelfP(srcObj.getP());
					node.accumulateSelfTypeCounts(srcObj.getMaxType(), 1);
				} else if (nodeObj.getBoolean("isAuthor")) {
					node.incrementOutCount();
					node.accumulateOutP(srcObj.getP());
					node.accumulateOutTypeCounts(srcObj.getMaxType(), 1);
				} else {
					node.incrementInCount();
					node.accumulateInP(srcObj.getP());
					node.accumulateInTypeCounts(srcObj.getMaxType(), 1);
				}
			}
			
			byte[] aggStr = node.getJSONAggregate().toString().getBytes();
			this.fullText.append(aggStr, 0, aggStr.length);
			context.write(key, this.fullText);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HConstructCorpNet");
		job.setJarByClass(HConstructCorpNet.class);
		job.setMapperClass(HConstructCorpNetMapper.class);
		job.setReducerClass(HConstructCorpNetReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


