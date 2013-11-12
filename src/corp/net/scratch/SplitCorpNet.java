package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import corp.data.CorpMetaData;
import corp.net.util.CorpNetProperties;
import edu.stanford.nlp.util.Pair;

import ark.data.Gazetteer;
import ark.util.FileUtil;

/* FIXME: This no longer works after changes to network construction */
public class SortNetworkData {
	private static CorpNetProperties properties = new CorpNetProperties();
	private static CorpMetaData corpMetaData = new CorpMetaData("Corp", properties.getCorpMetaDataPath());
	private static Gazetteer corpMetaDataGazetteer = new Gazetteer("CorpMetaData", properties.getCorpMetaDataGazetteerPath());
	
	public static void main(String[] args) {
		CorpNetProperties properties = new CorpNetProperties();
		String source = args[0];
		File outputDir = new File(properties.getNetworkDirPath(), source);
		File sourceFile = new File(properties.getNetworkSourceDirPath(), source);
		Map<String, Map<String, String>> netToOrgs = new HashMap<String, Map<String, String>>();
		
		if (!outputDir.exists() && !outputDir.mkdir()) {
			System.out.println("Failed to create output directory: " + outputDir.getAbsolutePath() + "... exiting.");
			return;
		}
		
		try {
			BufferedReader br = FileUtil.getFileReader(sourceFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineValues = line.trim().split("\\t");
				if (lineValues.length < 3)
					continue;
				
				String edgeKey = lineValues[0].trim();
				String[] edgeKeyParts = edgeKey.split("\\.");
				if (edgeKeyParts.length < 3)
					continue;	
				
				File edgeOutputDir = new File(outputDir.getAbsolutePath(), edgeKeyParts[0]);
				String edgeJsonStr = lineValues[1].trim();
				String edgeSourcesStr = lineValues[2].trim();
				
				Pair<String, String> nonnormalizedOrgs = getNonnormalizedOrgs(edgeSourcesStr);
				String nonnormalizedAuthor = nonnormalizedOrgs.first();
				String nonnormalizedMention = nonnormalizedOrgs.second();
				
				if (!netToOrgs.containsKey(edgeKeyParts[0]))
					netToOrgs.put(edgeKeyParts[0], new HashMap<String, String>());
				if (!netToOrgs.get(edgeKeyParts[0]).containsKey(edgeKeyParts[1]))
					netToOrgs.get(edgeKeyParts[0]).put(edgeKeyParts[1], nonnormalizedAuthor);
				if (!netToOrgs.get(edgeKeyParts[0]).containsKey(edgeKeyParts[2]))
					netToOrgs.get(edgeKeyParts[0]).put(edgeKeyParts[2], nonnormalizedMention);
				
				if (!outputEdge(edgeOutputDir, edgeKey, edgeJsonStr, edgeSourcesStr))
					return;
			}
			
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		outputOrgs(outputDir, netToOrgs);
	}
	
	private static Pair<String, String> getNonnormalizedOrgs(String edgeSourcesStr) {
		JSONArray jsonSources = JSONArray.fromObject(edgeSourcesStr);
		JSONObject firstSource = jsonSources.getJSONObject(0);
		
		return new Pair<String, String>(firstSource.getString("author"),
										firstSource.getJSONArray("mentions").getJSONObject(0).getString("text")
				);
	}
	
	private static boolean outputOrgs(File outputDir, Map<String, Map<String, String>> netToOrgs) {
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			System.out.println("Failed to make output directory: " + outputDir.getAbsolutePath());
			return false;
		}
		
		try {
			for (Entry<String, Map<String, String>> entry : netToOrgs.entrySet()) {
				File outputOrgsFile = new File(outputDir.getAbsolutePath(), entry.getKey() + "/orgs");
				BufferedWriter w = new BufferedWriter(new FileWriter(outputOrgsFile));
				for (Entry<String, String> orgEntry : entry.getValue().entrySet()) {
					JSONObject jsonOrg = new JSONObject();
					jsonOrg.put("text", orgEntry.getValue());
					jsonOrg.put("sics", getCorpSICs(orgEntry.getValue()));
					w.write(orgEntry.getKey() + "\t" + jsonOrg.toString() + "\n");
				}
				w.close();
			}
		} catch (Exception e) {
			e.printStackTrace(); return false;
		}
		
		return true;
	}
	
	private static JSONArray getCorpSICs(String corp) {
		List<String> ids = corpMetaDataGazetteer.getIds(corp);
		JSONArray sicsJSON = new JSONArray();
		for (String id : ids) {
			List<String> sicsForId = corpMetaData.getAttributeById(id, CorpMetaData.Attribute.SIC);
			sicsJSON.addAll(sicsForId);
		}		
		return sicsJSON;
	}
	
	private static boolean outputEdge(File outputDir, String edgeKey, String edge, String edgeSources) {
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			System.out.println("Failed to make output directory: " + outputDir.getAbsolutePath());
			return false;
		}
		
		File outputEdgesFile = new File(outputDir.getAbsolutePath(), "edges");
		File outputSourcesFile = new File(outputDir.getAbsolutePath(), "edgeSources");
		
        try {
        	System.out.println("Output edge " + edgeKey + ".");
        	
    		BufferedWriter w = new BufferedWriter(new FileWriter(outputEdgesFile, true));
    		w.write(edgeKey + "\t" + edge + "\n");
			w.close();
			
    		w = new BufferedWriter(new FileWriter(outputSourcesFile, true));
    		w.write(edgeKey + "\t" + edgeSources + "\n");
			w.close();
			return true;
        } catch (IOException e) { e.printStackTrace(); return false; }
	}
}
