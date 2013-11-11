package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import ark.util.FileUtil;

import corp.net.util.CorpNetProperties;

/* FIXME: This no longer works after changes to network construction */
public class FormatORANetworkData {
	private static class NetworkEdge {
		private String source;
		private String target;
		private Map<String, Double> typeValues;
		private int count;

		public NetworkEdge(String source, String target, Map<String, Double> typeValues, int count) {
			this.source = source;
			this.target = target;
			this.typeValues = typeValues;
			this.count = count;
		}
		
		public String getSource() {
			return this.source;
		}
		
		public String getTarget() {
			return this.target;
		}
		
		public int getCount() {
			return this.count;
		}
		
		public Map<String, Double> getTypeValues() {
			return this.typeValues;
		}
		
		@SuppressWarnings("rawtypes")
		public static NetworkEdge fromString(String str) {
			String[] strParts = str.split("\\t");
			if (strParts.length < 2) {
				return null;
			}
			
			String edgeKey = strParts[0];
			String[] edgeKeyParts = edgeKey.split("\\.");
			String org1 = edgeKeyParts[1];
			String org2 = edgeKeyParts[2];
			
			String edgeValue = strParts[1];
			JSONObject edgeObj = JSONObject.fromObject(edgeValue);
			JSONObject pObj = edgeObj.getJSONObject("p");
			int count = edgeObj.getInt("count");
			Set p = pObj.entrySet();
			Map<String, Double> typeValues = new TreeMap<String, Double>();
			for (Object pValueObj : p) {
				Entry pValue = (Entry)pValueObj;
				String type = pValue.getKey().toString();
				double value = Double.parseDouble(pValue.getValue().toString());
				typeValues.put(type, value);
			}
			
			return new FormatORANetworkData.NetworkEdge(org1, org2, typeValues, count);
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
			File networkFile = new File(networkDir.getAbsolutePath(), "net");
			File outputFile = new File(networkDir.getAbsolutePath(), "ora_" + networkDir.getParentFile().getName() + "_" + networkName);
			if (!convertToORA(networkFile, outputFile, networkName)) {
				System.out.println("Failed to summarize " + networkFile.getAbsolutePath() + ".");
				return;
			}
		}
	}
	
	private static boolean convertToORA(File inputFile, File outputFile, String networkName) {
        try {
    		BufferedWriter w = new BufferedWriter(new FileWriter(outputFile));
    		Set<String> organizations = new TreeSet<String>();
    		Set<String> edgeTypes = new TreeSet<String>();
    		if (!getOrgsAndEdgeTypes(inputFile, organizations, edgeTypes)) {
    			w.close();
    			return false;
    		}
    		w.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n");
    		w.write("<DynamicNetwork>\n");
    	    w.write("\t<MetaMatrix " + (networkName.matches("[0-9]*") ? "timePeriod=\"" + networkName + "\"" : "" ) + ">\n");
    	    w.write("\t\t<nodes>\n");
    	    w.write("\t\t\t<nodeset type=\"organization\" id=\"organization\">\n");
    	    
    	    for (String organization : organizations) {
    	    	w.write("\t\t\t\t<node id=\"" + organization + "\" />\n");
    	    }
    	    
    		w.write("\t\t\t</nodeset>\n");
    		w.write("\t\t</nodes>\n");
    		w.write("\t\t<networks>\n");
    		
    		for (String edgeType : edgeTypes) {
	    		w.write("\t\t\t<graph sourceType=\"organization\" targetType=\"organization\" id=\"" + edgeType + "\">\n");
	    		
				BufferedReader br = FileUtil.getFileReader(inputFile.getAbsolutePath());
				String line = null;
				while ((line = br.readLine()) != null) {
					NetworkEdge edge = NetworkEdge.fromString(line);
					if (edge == null)
						continue;
					if (!edge.getTypeValues().containsKey(edgeType))
						continue;
					w.write("\t\t\t\t<edge source=\"" + edge.getSource() + 
							"\" target=\"" + edge.getTarget() + 
							"\" type=\"double\" value=\"" + edge.getTypeValues().get(edgeType) + 
							"\" />\n");	
				}
				
				br.close();
	    		
	    		w.write("\t\t\t</graph>\n");
    		}
    		
    		w.write("\t\t\t<graph sourceType=\"organization\" targetType=\"organization\" id=\"Mention\">\n");
			BufferedReader br = FileUtil.getFileReader(inputFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				NetworkEdge edge = NetworkEdge.fromString(line);
				if (edge == null)
					continue;
				w.write("\t\t\t\t<edge source=\"" + edge.getSource() + 
						"\" target=\"" + edge.getTarget() + 
						"\" type=\"double\" value=\"" + edge.getCount() + 
						"\"/>\n");	
			}
			
			br.close();
    		w.write("\t\t\t</graph>\n");
    		
    		w.write("\t\t</networks>\n");
    		w.write("\t</MetaMatrix>\n");
    		w.write("</DynamicNetwork>\n");
    		
			w.close();
			return true;
        } catch (IOException e) { e.printStackTrace(); return false; }
	}
	
	private static boolean getOrgsAndEdgeTypes(File inputFile, Set<String> organizations, Set<String> edgeTypes) {
		try {
			BufferedReader br = FileUtil.getFileReader(inputFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				NetworkEdge edge = NetworkEdge.fromString(line);
				if (edge == null)
					return false;
				organizations.add(edge.getSource());
				organizations.add(edge.getTarget());
				edgeTypes.addAll(edge.getTypeValues().keySet());
			}
			
			br.close();
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
