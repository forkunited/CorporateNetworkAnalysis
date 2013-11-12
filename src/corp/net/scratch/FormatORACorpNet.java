package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Set;
import java.util.TreeSet;

import ark.util.FileUtil;

import corp.net.util.CorpNetProperties;

public class FormatORANetworkData {	
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
