package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import ark.util.FileUtil;

import corp.net.CorpNetEdge;
import corp.net.CorpNetNode;
import corp.net.util.CorpNetProperties;

/**
 * 
 * FormatORACorpNet converts the output of corp.net.scratch.SplitCorpNet
 * to the format used by the ORA network analysis tool 
 * (http://www.casos.cs.cmu.edu/projects/ora/).
 * 
 * @author Bill McDowell
 *
 */
public class FormatORACorpNet {	
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
			File outputFile = new File(networkDir.getAbsolutePath(), "ORA_" + networkDir.getParentFile().getName() + "_" + networkName);
			if (!convertToORA(networkDir, outputFile, networkName)) {
				System.out.println("Failed to summarize network " + networkName + ".");
				return;
			}
		}
	}
	
	private static boolean convertToORA(File inputDir, File outputFile, String networkName) {
		File inputNodesFile = new File(inputDir.getAbsolutePath(), "NODES");
		File inputEdgesFile = new File(inputDir.getAbsolutePath(), "EDGES");
		
        try {
    		BufferedWriter w = new BufferedWriter(new FileWriter(outputFile));
    		Set<String> edgeTypes = new TreeSet<String>();
    		if (!getEdgeTypes(inputEdgesFile, edgeTypes)) {
    			w.close();
    			return false;
    		}
    		w.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n");
    		w.write("<DynamicNetwork>\n");
    	    w.write("\t<MetaMatrix " + (networkName.matches("[0-9]*") ? "timePeriod=\"" + networkName + "\"" : "" ) + ">\n");
    	    w.write("\t\t<nodes>\n");
    	    w.write("\t\t\t<nodeset type=\"organization\" id=\"organization\">\n");
    	    
			BufferedReader br = FileUtil.getFileReader(inputNodesFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				CorpNetNode node = CorpNetNode.fromString(line);
				w.write("\t\t\t\t<node id=\"" + node.getNode() + "\" />\n");
			}
			
			br.close();
    	    
    		w.write("\t\t\t</nodeset>\n");
    		w.write("\t\t</nodes>\n");
    		w.write("\t\t<networks>\n");
    		
    		for (String edgeType : edgeTypes) {
	    		w.write("\t\t\t<graph sourceType=\"organization\" targetType=\"organization\" id=\"" + edgeType + "\">\n");
	    		
				br = FileUtil.getFileReader(inputEdgesFile.getAbsolutePath());
				line = null;
				while ((line = br.readLine()) != null) {
					CorpNetEdge edge = CorpNetEdge.fromString(line);
					
					if (edge.getForwardP().containsKey(edgeType))
						w.write("\t\t\t\t<edge source=\"" + edge.getNode1() + 
								"\" target=\"" + edge.getNode2() + 
								"\" type=\"double\" value=\"" + edge.getForwardP().get(edgeType) + 
								"\" />\n");	
					
					if (edge.getBackwardP().containsKey(edgeType))
						w.write("\t\t\t\t<edge source=\"" + edge.getNode2() + 
								"\" target=\"" + edge.getNode1() + 
								"\" type=\"double\" value=\"" + edge.getBackwardP().get(edgeType) + 
								"\" />\n");	
				}
				
				br.close();
	    		
	    		w.write("\t\t\t</graph>\n");
    		}
    		
    		w.write("\t\t\t<graph sourceType=\"organization\" targetType=\"organization\" id=\"Mention\">\n");
			br = FileUtil.getFileReader(inputEdgesFile.getAbsolutePath());
			line = null;
			while ((line = br.readLine()) != null) {
				CorpNetEdge edge = CorpNetEdge.fromString(line);
				
				w.write("\t\t\t\t<edge source=\"" + edge.getNode1() + 
						"\" target=\"" + edge.getNode2() + 
						"\" type=\"double\" value=\"" + edge.getForwardCount() + 
						"\"/>\n");	
				w.write("\t\t\t\t<edge source=\"" + edge.getNode2() + 
						"\" target=\"" + edge.getNode1() + 
						"\" type=\"double\" value=\"" + edge.getBackwardCount() + 
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
	
	private static boolean getEdgeTypes(File inputEdgesFile, Set<String> edgeTypes) {
		try {
			BufferedReader br = FileUtil.getFileReader(inputEdgesFile.getAbsolutePath());
			String line = null;
			while ((line = br.readLine()) != null) {
				CorpNetEdge edge = CorpNetEdge.fromString(line);
				edgeTypes.addAll(edge.getForwardP().keySet());
				edgeTypes.addAll(edge.getBackwardP().keySet());
			}
			
			br.close();
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
