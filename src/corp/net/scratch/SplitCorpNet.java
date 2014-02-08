package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import corp.net.CorpNetObject;
import corp.net.util.CorpNetProperties;
import ark.util.FileUtil;

/**
 * SplitCorpNet takes the bulk output from corp.net.hadoop.HConstructCorpNet,
 * and splits it into a sub-directories containing files for each network.
 * Every network N (for each year, FULL, etc) gets its own sub-directory
 * of the same name containing the following files:
 * 
 * <N>/DOC (Document lines output by HConstructCorpNet)
 * <N>/NODE (Node lines output by HConstructCorpNet)
 * <N>/EDGE (Edge lines output by HConstructCorpNet)
 * 
 * @author Bill McDowell
 *
 */
public class SplitCorpNet {
	private static CorpNetProperties properties = new CorpNetProperties();
	
	public static void main(String[] args) {
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
				CorpNetObject netObj = CorpNetObject.fromString(line);
				
				File netOutputDir = new File(outputDir.getAbsoluteFile(), netObj.getNet());
				if (!netOutputDir.exists() && !netOutputDir.mkdir()) {
					System.out.println("Failed to create output directory: " + netOutputDir.getAbsolutePath() + "... exiting.");
					return;
				}
				
				File outputFile = new File(netOutputDir.getAbsoluteFile(), netObj.getType().toString());
				BufferedWriter w = new BufferedWriter(new FileWriter(outputFile, true));
				w.write(line + "\n");
				w.close();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
}
