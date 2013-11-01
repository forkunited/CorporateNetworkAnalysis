package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import corp.net.util.CorpNetProperties;

import ark.util.FileUtil;

public class ConstructNetworkData {
	public static void main(String[] args) {
		CorpNetProperties properties = new CorpNetProperties();
		String source = args[0];
		File outputDir = new File(properties.getNetworkDirPath(), source);
		File sourceFile = new File(properties.getNetworkSourceDirPath(), source);
		
		if (!outputDir.exists() && !outputDir.mkdir()) {
			System.out.println("Failed to create output directory: " + outputDir.getAbsolutePath() + "... exiting.");
			return;
		}
		
		try {
			BufferedReader br = FileUtil.getFileReader(sourceFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineValues = line.trim().split("\\t");
				if (lineValues.length < 2)
					continue;
				
				String edgeKey = lineValues[0].trim();
				String[] edgeKeyParts = edgeKey.split("\\.");
				if (edgeKeyParts.length < 3)
					continue;		
				
				File edgeOutputDir = new File(outputDir.getAbsolutePath(), edgeKeyParts[0]);
				String edgeJsonStr = lineValues[1].trim();
				String edgeSourcesStr = lineValues[2].trim();
				if (!outputEdge(edgeOutputDir, edgeKey, edgeJsonStr, edgeSourcesStr))
					return;
			}
			
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private static boolean outputEdge(File outputDir, String edgeKey, String edge, String edgeSources) {
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			System.out.println("Failed to make output directory: " + outputDir.getAbsolutePath());
			return false;
		}
		
		File outputNetFile = new File(outputDir.getAbsolutePath(), "net");
		File outputSourcesFile = new File(outputDir.getAbsolutePath(), "sources");
		
        try {
        	System.out.println("Output edge " + edgeKey + ".");
        	
    		BufferedWriter w = new BufferedWriter(new FileWriter(outputNetFile));
    		w.write(edgeKey + "\t" + edge + "\n");
			w.close();
			
    		w = new BufferedWriter(new FileWriter(outputSourcesFile));
    		w.write(edgeKey + "\t" + edgeSources + "\n");
			w.close();
			return true;
        } catch (IOException e) { e.printStackTrace(); return false; }
	}
}
