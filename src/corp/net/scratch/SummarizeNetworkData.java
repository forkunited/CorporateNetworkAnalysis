package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.sf.json.JSONObject;

import ark.util.FileUtil;

import corp.net.util.CorpNetProperties;

/* FIXME: This no longer works after changes to network construction */
public class SummarizeNetworkData {
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
			File networkFile = new File(networkDir.getAbsolutePath(), "net");
			File outputFile = new File(networkDir.getAbsolutePath(), "summary");
			if (!summarizeDataFromFile(networkFile, outputFile)) {
				System.out.println("Failed to summarize " + networkFile.getAbsolutePath() + ".");
				return;
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private static boolean summarizeDataFromFile(File networkFile, File outputFile) {
		Set<String> orgs = new HashSet<String>();
		Set<String> authors = new HashSet<String>();
		Map<String, Double> relationSums = new TreeMap<String, Double>();
		int mentionSum = 0;
		int relationCount = 0;
		
		try {
			BufferedReader br = FileUtil.getFileReader(networkFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				if (lineParts.length < 2) {
					System.out.println("Invalid input line: " + line);
					return false;
				}
				String edgeKey = lineParts[0];
				String[] edgeKeyParts = edgeKey.split("\\.");
				String org1 = edgeKeyParts[1];
				String org2 = edgeKeyParts[2];
				
				String edgeValue = lineParts[1];
				JSONObject edgeObj = JSONObject.fromObject(edgeValue);
				JSONObject pObj = edgeObj.getJSONObject("p");
				
				Set p = pObj.entrySet();
				for (Object pValueObj : p) {
					Entry pValue = (Entry)pValueObj;
					String key = pValue.getKey().toString();
					double value = Double.parseDouble(pValue.getValue().toString());
				
					if (!relationSums.containsKey(key))
						relationSums.put(key, 0.0);
					relationSums.put(key, relationSums.get(key) + value);
				}
				
				authors.add(org1);
				orgs.add(org1);
				orgs.add(org2);
				mentionSum +=edgeObj.getInt("count");
				relationCount++;
			}
			
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
        try {
    		BufferedWriter w = new BufferedWriter(new FileWriter(outputFile));
    		for (Entry<String, Double> relationSum : relationSums.entrySet())
    			w.write("Relation " +  relationSum.getKey() + "\t" + relationSum.getValue() + "\n");
    		w.write("Author Total\t" + authors.size() + "\n");
    		w.write("Organization Total\t" + orgs.size() + "\n");
    		w.write("Mention Total\t" + mentionSum + "\n");
    		w.write("Relation Total\t" + relationCount + "\n");

			w.close();
			return true;
        } catch (IOException e) { e.printStackTrace(); return false; }
	}
}
