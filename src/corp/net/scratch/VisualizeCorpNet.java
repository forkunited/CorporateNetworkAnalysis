package corp.net.scratch;

import java.io.BufferedReader;
import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import ark.util.FileUtil;
import corp.net.CorpNetEdge;
import corp.net.CorpNetEdgeSource;
import corp.net.CorpNetNode;
import corp.net.util.CorpNetProperties;
import corp.net.util.MathUtil;

public class VisualizeCorpNet {
	private static String VISUALIZE_URL = "http://demo.ark.cs.cmu.edu/cre/server/storeMessGraph.php";
	private static int MESSAGES_PER_BATCH = 50;
	private static DecimalFormat DOUBLE_FORMAT = new DecimalFormat("#.##");
	
	private static class KeyTermDictionary {
		private Map<String, Set<String>> keyTerms;
		
		public KeyTermDictionary() {
			this.keyTerms = new HashMap<String, Set<String>>();
		}
		
		public void addTerm(String key, double value) {
			addTerm(key, String.valueOf(value));
		}
		
		public void addTerms(String key, List<String> values) {
			if (values == null)
				return;
			for (String value : values) {
				addTerm(key, value);
			}
		}
		
		public void addTerm(String key, String term) {
			if (term == null)
				return;
			if (!this.keyTerms.containsKey(key))
				this.keyTerms.put(key, new TreeSet<String>());
			this.keyTerms.get(key).add(term);
		}
		
		public String toString() {
			StringBuilder str = new StringBuilder();
			
			for (Entry<String, Set<String>> entry : this.keyTerms.entrySet()) {
				for (String term : entry.getValue()) {
					str = str.append(entry.getKey()).append("_").append(term).append(" ");
				}
			}
			
			return str.toString();
		}
	}
	
	public static void main(String[] args) {
		CorpNetProperties properties = new CorpNetProperties();
		String source = args[0];
		File inputDir = new File(properties.getNetworkDirPath(), source);
		
		if (!inputDir.exists()) {
			System.out.println("Input directory " + inputDir.getAbsolutePath() + " does not exist. Exiting...");
			return;
		}
		
		File[] networkDirs = inputDir.listFiles();
		for (File networkDir : networkDirs) {
			if (!networkDir.isDirectory())
				continue;
			String networkName = networkDir.getName();
			if (!createCorporateMess(networkDir, networkName)) {
				System.out.println("Failed to output visualizatin construction file for network " + networkName + ".");
				return;
			}
		}
	}
	
	private static boolean createCorporateMess(File inputDir, String networkName) {
		if (!networkName.equals("FULL"))
			return true;
		
		System.out.println("Creating visualization for " + networkName + ".");
		
		Map<String, String> nodesToTagIds = createTagsForNodes(inputDir, networkName);
		if (nodesToTagIds == null) {
			System.out.println("Error: Failed to create tags for nodes in network " + networkName + ".");
			return false;
		}
		
		Map<String, String> nodesToNodeIds = createNodes(inputDir, networkName, nodesToTagIds);
		if (nodesToNodeIds == null) {
			System.out.println("Error: Failed to create nodes in network " + networkName + ".");
			return false;
		}
		
		/*if (!createRelationships(inputDir, networkName, nodesToNodeIds)) {
			System.out.println("Error: Failed to create relationships in network " + networkName + ".");
			return false;
		} Add back later */
		
		return true;
	}
	
	private static Map<String, String> createTagsForNodes(File inputDir, String networkName) {
		System.out.println("Creating tags for nodes in " + networkName + ".");
		Map<String, String> nodesToTagIds = new HashMap<String, String>();
		File inputNodesFile = new File(inputDir.getAbsolutePath(), "NODE");
        try {
        	System.out.println("Reading input file " + inputNodesFile.getAbsolutePath() + "...");
			BufferedReader br = FileUtil.getFileReader(inputNodesFile.getAbsolutePath());
			String line = null;
			JSONArray messages = new JSONArray();
			while ((line = br.readLine()) != null) {
				CorpNetNode node = CorpNetNode.fromString(line);
				
				System.out.println("Creating tag message for node " + node.getNode() + " in " + networkName + ".");
				JSONObject tagMessage = createTagMessage(node.getNet(), node.getNode());
				messages.add(tagMessage);
				
				if (messages.size() == MESSAGES_PER_BATCH) {
					System.out.println("Sending tag message batch for tags in " + networkName + ".");
					JSONArray responses = sendRequest(networkName, messages);
					if (responses == null)
						return null;
					
					for (int i = 0; i < responses.size(); i++) {
						nodesToTagIds.put(messages.getJSONObject(i).getJSONObject("tag").getString("name"), responses.getJSONObject(i).getString("id"));
					}
					
					messages = new JSONArray();
					
					return nodesToTagIds; // FIXME Remove later
				}
			}
			
			if (messages.size() > 0) {
				System.out.println("Sending tag message batch for tags in " + networkName + ".");
				JSONArray responses = sendRequest(networkName, messages);
				if (responses == null)
					return null;
				
				for (int i = 0; i < responses.size(); i++) {
					nodesToTagIds.put(messages.getJSONObject(i).getJSONObject("tag").getString("name"), responses.getJSONObject(i).getString("id"));
				}
			}
			
			br.close();
        } catch (Exception e) {
        	e.printStackTrace();
        	return null;
        }
		
		return nodesToTagIds;
	}
	
	private static Map<String, String> createNodes(File inputDir, String networkName, Map<String, String> nodesToTagIds) {
		System.out.println("Creating nodes in " + networkName + ".");
		
		Map<String, String> nodesToNodeIds = new HashMap<String, String>();
		File inputNodesFile = new File(inputDir.getAbsolutePath(), "NODE");
        try {
        	System.out.println("Reading input file " + inputNodesFile.getAbsolutePath() + "...");
			BufferedReader br = FileUtil.getFileReader(inputNodesFile.getAbsolutePath());
			String line = null;
			JSONArray messages = new JSONArray();
			double[] stepValues = {0, 1, 10, 100, 1000, 10000, 100000};
			while ((line = br.readLine()) != null) {
				CorpNetNode node = CorpNetNode.fromString(line);
				System.out.println("Creating node message for node " + node.getNode() + " in " + networkName + ".");
				
				KeyTermDictionary nodeKeyTerms = new KeyTermDictionary();
				nodeKeyTerms.addTerm("inCount", getStepValue(node.getInCount(), stepValues));
				nodeKeyTerms.addTerm("inPMax", MathUtil.argMaxDistribution(node.getInP()));
				nodeKeyTerms.addTerm("inTypeCountsMax", MathUtil.argMaxHistogram(node.getInTypeCounts()));
				nodeKeyTerms.addTerm("outCount", getStepValue(node.getOutCount(), stepValues));
				nodeKeyTerms.addTerm("outPMax", MathUtil.argMaxDistribution(node.getOutP()));
				nodeKeyTerms.addTerm("outTypeCountsMax",  MathUtil.argMaxHistogram(node.getOutTypeCounts()));
				nodeKeyTerms.addTerm("selfCount", getStepValue(node.getSelfCount(), stepValues));
				nodeKeyTerms.addTerm("selfPMax",  MathUtil.argMaxDistribution(node.getSelfP()));
				nodeKeyTerms.addTerm("selfTypeCountsMax", MathUtil.argMaxHistogram(node.getSelfTypeCounts()));
				nodeKeyTerms.addTerms("ciks", node.getMetaDataCiks());
				nodeKeyTerms.addTerms("countries", node.getMetaDataCountries());
				nodeKeyTerms.addTerms("industries", node.getMetaDataIndustries());
				nodeKeyTerms.addTerms("sics", node.getMetaDataSics());
				nodeKeyTerms.addTerms("tickers", node.getMetaDataTickers());
				nodeKeyTerms.addTerms("types", node.getMetaDataTypes());
				
				StringBuilder thorough = new StringBuilder();
				thorough = thorough.append("Search Terms: ").append(nodeKeyTerms.toString()).append("\n");
				
				thorough = thorough.append("In-Mention Count: ").append(node.getInCount()).append("\n");
				thorough = thorough.append("Out-Mention Count: ").append(node.getOutCount()).append("\n");
				thorough = thorough.append("Self-Mention Count: ").append(node.getSelfCount()).append("\n");
				thorough = thorough.append("\n");
				thorough = thorough.append("Ciks: ").append(getListString(node.getMetaDataCiks())).append("\n");
				thorough = thorough.append("Countries: ").append(getListString(node.getMetaDataCountries())).append("\n");
				thorough = thorough.append("Industries: ").append(getListString(node.getMetaDataIndustries())).append("\n");
				thorough = thorough.append("Sics: ").append(getListString(node.getMetaDataSics())).append("\n");
				thorough = thorough.append("Tickers: ").append(getListString(node.getMetaDataTickers())).append("\n");
				thorough = thorough.append("Types: ").append(getListString(node.getMetaDataTypes())).append("\n");
				thorough = thorough.append("\n");
				thorough = thorough.append("In-P (Count): ").append("\n");
				thorough = thorough.append(getDistributionString(node.getInP(), node.getInTypeCounts()));
				thorough = thorough.append("\n");
				thorough = thorough.append("Out-P (Count): ");
				thorough = thorough.append(getDistributionString(node.getOutP(), node.getOutTypeCounts()));
				thorough = thorough.append("\n");
				thorough = thorough.append("Self-P (Count): ");
				thorough = thorough.append(getDistributionString(node.getSelfP(), node.getSelfTypeCounts()));
				thorough = thorough.append("\n");
				
				JSONObject nodeMessage = createNodeMessage(nodesToTagIds.get(node.getNode()) + "_0", nodesToTagIds.get(node.getNode()), node.getNode(), thorough.toString());
				messages.add(nodeMessage);
				
				if (messages.size() == MESSAGES_PER_BATCH) {
					System.out.println("Sending node message batch for nodes in " + networkName + ".");
					JSONArray responses = sendRequest(networkName, messages);
					if (responses == null)
						return null;
					
					for (int i = 0; i < responses.size(); i++) {
						nodesToNodeIds.put(messages.getJSONObject(i).getJSONObject("node").getString("brief"), responses.getJSONObject(i).getString("id"));
					}
					
					messages = new JSONArray();
					
					return nodesToNodeIds; // FIXME Remove later
				}
			
			}
			
			if (messages.size() > 0) {
				System.out.println("Sending node message batch for nodes in " + networkName + ".");
				JSONArray responses = sendRequest(networkName, messages);
				if (responses == null)
					return null;
				
				for (int i = 0; i < responses.size(); i++) {
					nodesToNodeIds.put(messages.getJSONObject(i).getJSONObject("node").getString("brief"), responses.getJSONObject(i).getString("id"));
				}
			}
			
			br.close();
        } catch (Exception e) {
        	e.printStackTrace();
        	return null;
        }
		
		return nodesToNodeIds;
	}
	
	private static boolean createRelationships(File inputDir, String networkName, Map<String, String> nodesToNodeIds) {
		System.out.println("Creating relationships in " + networkName + ".");
		
		File inputEdgesFile = new File(inputDir.getAbsolutePath(), "EDGE");
        try {
        	System.out.println("Reading input file " + inputEdgesFile.getAbsolutePath() + "...");
			BufferedReader br = FileUtil.getFileReader(inputEdgesFile.getAbsolutePath());
			String line = null;
			JSONArray messages = new JSONArray();
			double[] stepValues = {0, 1, 10, 100, 1000, 10000, 100000};
			while ((line = br.readLine()) != null) {
				CorpNetEdge edge = CorpNetEdge.fromString(line);
				System.out.println("Creating edge message for edge " + edge.getNode1() + "_" + edge.getNode2() + " in " + networkName + ".");
				int direction = 0;
				int group = 0;
				String maxType = null;
				Map<String, Integer> typesToGroups = getEdgeTypesToGroups(edge.getForwardP().keySet(), edge.getBackwardP().keySet());
				if (edge.getForwardCount() > 0 && edge.getBackwardCount() > 0) {
					direction = 0;
					String maxForwardType = MathUtil.argMaxDistribution(edge.getForwardP());
					String maxBackwardType = MathUtil.argMaxDistribution(edge.getBackwardP());
					if (edge.getForwardP().get(maxForwardType) > edge.getBackwardP().get(maxBackwardType)) {
						group = typesToGroups.get(maxForwardType);
						maxType = maxForwardType;
					} else {
						group = typesToGroups.get(maxBackwardType);
						maxType = maxBackwardType;
					}
				} else if (edge.getBackwardCount() > 0) {
					direction = 2;
					maxType = MathUtil.argMaxDistribution(edge.getBackwardP());
					group = typesToGroups.get(maxType);
				} else {
					direction = 1;
					maxType = MathUtil.argMaxDistribution(edge.getForwardP());
					group = typesToGroups.get(maxType);
				}

				KeyTermDictionary edgeKeyTerms = new KeyTermDictionary();
				edgeKeyTerms.addTerm("edgeMaxType", maxType);
				edgeKeyTerms.addTerm("edgeMentionCount", getStepValue(edge.getForwardCount()+edge.getBackwardCount(), stepValues));
				
				StringBuilder thorough = new StringBuilder();
				thorough = thorough.append("Search Terms: ").append(edgeKeyTerms.toString()).append("\n");
				
				thorough = thorough.append("Most Likely Type: ").append(maxType).append("\n");
				thorough = thorough.append("Total Mentions: ").append(edge.getForwardCount()+edge.getBackwardCount()).append("\n");
				thorough = thorough.append("Forward Mention Count: ").append(edge.getForwardCount()).append("\n");
				thorough = thorough.append("Backward Mention Count: ").append(edge.getBackwardCount()).append("\n");
				thorough = thorough.append("Forward-P:").append("\n");
				thorough = thorough.append(getDistributionString(edge.getForwardP(), null));
				thorough = thorough.append("\n");
				thorough = thorough.append("Backward-P:").append("\n");
				thorough = thorough.append(getDistributionString(edge.getBackwardP(), null));
				thorough = thorough.append("\n");
				thorough = thorough.append("Sources: \n");
				List<CorpNetEdgeSource> sources = edge.getSources();
				for (CorpNetEdgeSource source : sources) {
					thorough = thorough.append(source.toHTMLString()).append("\n\n");
				}
				
				JSONObject relationshipMessage = createRelationshipMessage(nodesToNodeIds.get(edge.getNode1()), nodesToNodeIds.get(edge.getNode2()), 0, group, direction, thorough.toString());
				messages.add(relationshipMessage);
				
				if (messages.size() == MESSAGES_PER_BATCH) {
					System.out.println("Sending edge message batch for edges in " + networkName + ".");
					JSONArray responses = sendRequest(networkName, messages);
					if (responses == null)
						return false;
					
					messages = new JSONArray();
				}
			}

			if (messages.size() == MESSAGES_PER_BATCH) {
				System.out.println("Sending edge message batch for edges in " + networkName + ".");
				JSONArray responses = sendRequest(networkName, messages);
				if (responses == null)
					return false;
			}
			
			br.close();
        } catch (Exception e) {
        	e.printStackTrace();
        	return false;
        }
		
		return true;
	}
	
	private static JSONObject createTagMessage(String user, String name) {
		JSONObject tag = new JSONObject();
		tag.put("id", "tagTempId");
		tag.put("creatorUserId", user);
		tag.put("name", name);
		
		JSONObject tagMessage = new JSONObject();
		tagMessage.put("messageType", "requestAddTag");
		tagMessage.put("tag", tag);
		
		return tagMessage;
	}
	
	private static JSONObject createNodeMessage(String id, String tagId, String brief, String thorough) {
		JSONObject node = new JSONObject();
		node.put("id", id);
		node.put("tagId", tagId);
		node.put("brief", brief);
		node.put("thorough", thorough);
		node.put("posX", 0);
		node.put("posY", 0);
		node.put("main", 0);
		
		JSONObject nodeMessage = new JSONObject();
		nodeMessage.put("messageType", "requestAddNode");
		nodeMessage.put("node", node);
		
		return nodeMessage;
	}

	private static JSONObject createRelationshipMessage(String id1, String id2, int type, int group, int direction, String thorough) {
		JSONObject relationship = new JSONObject();
		relationship.put("id1", id1);
		relationship.put("id2", id2);
		relationship.put("type", type);
		relationship.put("group", group);
		relationship.put("direction", direction);
		relationship.put("thorough", thorough);
		
		JSONObject relationshipMessage = new JSONObject();
		relationshipMessage.put("messageType", "requestAddNode");
		relationshipMessage.put("relationship", relationship);
		
		return relationshipMessage;
	}
	
	private static JSONArray sendRequest(String user, JSONArray messages) {
		try {
		    HttpClient client = new HttpClient();
		    PostMethod post = new PostMethod(VISUALIZE_URL);
		    post.addParameter("user", user);
		    post.addParameter("messages", messages.toString());
		    
		    int status = client.executeMethod(post);
		    if (status == 200) {
		    	String response = post.getResponseBodyAsString();
		    	JSONArray responseMessages = JSONArray.fromObject(response);
		    	
		    	for (int i = 0; i < responseMessages.size(); i++) {
		    		if (responseMessages.getJSONObject(i).getBoolean("failure")) {
		    			System.out.println("HTTP request message " + i + " in batch failed: " + responseMessages.getJSONObject(i).toString());
		    			return null;
		    		}
		    	}
		    	
		    	return responseMessages;
		    } else {
		    	System.out.println("Error: HTTP Request failed with error status " + status + ".");
		    	return null;
		    }
		} catch (Exception e) {
			System.out.println("Error: HTTP Request failed with exception: " + e.getMessage());
			return null;
		}
	}
	
	private static double getStepValue(double value, double[] validSteps) {
		for (int i = 0; i < validSteps.length; i++) {
			if (value >= validSteps[i])
				return validSteps[i];
		}
		return 0.0;
	}
	
	private static String getListString(List<String> strs) {
		StringBuilder retStr = new StringBuilder();
		for (String str : strs)
			retStr = retStr.append(str).append(",");
		if (retStr.length() > 0)
			retStr = retStr.delete(retStr.length() - 1, retStr.length());
		return retStr.toString();
	}
	
	private static String getDistributionString(Map<String, Double> distribution, Map<String, Integer> histogram) {
		StringBuilder retStr = new StringBuilder();
		
		for (Entry<String, Double> entry : distribution.entrySet()) {
			retStr = retStr.append(entry.getKey())
						   .append(": ")
						   .append(DOUBLE_FORMAT.format(entry.getValue()));
			if (histogram != null) {
				retStr = retStr.append(" (")
							   .append(histogram.get(entry.getKey()))
							   .append(")\n");
			} else {
				retStr = retStr.append("\n");
			}
		}
		
		return retStr.toString();
	}
	
	private static Map<String, Integer> getEdgeTypesToGroups(Set<String> forwardTypes, Set<String> backwardTypes) {
		TreeSet<String> sortedTypes = new TreeSet<String>();
		sortedTypes.addAll(forwardTypes);
		sortedTypes.addAll(backwardTypes);
		Map<String, Integer> typesToGroups = new HashMap<String, Integer>();
		int group = 0;
		for (String type : sortedTypes) {
			typesToGroups.put(type, group);
			group++;
		}
		return typesToGroups;
	}
}
