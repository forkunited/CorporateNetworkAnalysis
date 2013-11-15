package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import ark.util.FileUtil;
import corp.net.CorpNetObject;
import corp.net.util.CorpNetProperties;

public class SplitCorpNetSummary {
	private static CorpNetProperties properties = new CorpNetProperties();
	
	public static void main(String[] args) {
		/*String source = args[0];
		File outputDir = new File(properties.getNetworkDirPath(), source);
		File summarySourceFile = new File(properties.getNetworkSourceDirPath(), source + "_Summary");
		File summaryAggregationSourceFile = new File(properties.getNetworkSourceDirPath(), source + "_SummaryAggregation");
		
		if (!outputDir.exists() && !outputDir.mkdir()) {
			System.out.println("Failed to create output directory: " + outputDir.getAbsolutePath() + "... exiting.");
			return;
		}
		
		try {
			BufferedReader br = FileUtil.getFileReader(sourceFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				CorpNetObject netObj = CorpNetObject.fromString(line);
				File outputFile = new File(outputDir.getAbsoluteFile(), netObj.getNet() + "/" + netObj.getType());
				BufferedWriter w = new BufferedWriter(new FileWriter(outputFile, true));
				w.write(line + "\n");
				w.close();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}*/	
	}
}
