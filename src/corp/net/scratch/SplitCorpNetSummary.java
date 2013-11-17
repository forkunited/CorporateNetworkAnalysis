package corp.net.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import ark.util.FileUtil;
import corp.net.summary.CorpNetSummaryEntry;
import corp.net.util.CorpNetProperties;
import corp.net.util.JSONUtil;
import edu.stanford.nlp.util.Pair;

public class SplitCorpNetSummary {
	private static CorpNetProperties properties = new CorpNetProperties();
	
	public static void main(String[] args) {
		String source = args[0];
		File outputDir = new File(properties.getNetworkDirPath(), source);
		if (!outputDir.exists() && !outputDir.mkdir()) {
			System.out.println("Failed to create output directory: " + outputDir.getAbsolutePath() + "... exiting.");
			return;
		}
		
		outputSummaryRotation(outputDir, source);
		outputSummaryAggregation(outputDir, source);
	}
	
	public static void outputSummaryRotation(File outputDir, String source) {
		File summaryRotationSourceFile = new File(properties.getNetworkSourceDirPath(), source + "_SummaryRotation");
		try {
			BufferedReader br = FileUtil.getFileReader(summaryRotationSourceFile.getAbsolutePath());
			String line = null;
			Map<String, Set<String>> fileMeasureSubtypes = new HashMap<String, Set<String>>();
			while ((line = br.readLine()) != null) {
				Map<String, Double> values = new TreeMap<String, Double>();
				Pair<CorpNetSummaryEntry, File> info = getInfoForRotationLine(line, outputDir, values);
				File outputFile = info.second();
				
				if (!fileMeasureSubtypes.containsKey(outputFile.getAbsolutePath()))
					fileMeasureSubtypes.put(outputFile.getAbsolutePath(), new TreeSet<String>());
				
				for (Entry<String, Double> e : values.entrySet()) {
					fileMeasureSubtypes.get(outputFile.getAbsolutePath()).add(e.getKey());
				}
			}
			br.close();
			
			/* Output column headings */
			for (Entry<String, Set<String>> e : fileMeasureSubtypes.entrySet()) {
				File measureSubtypesFile = new File(e.getKey());
				BufferedWriter w = new BufferedWriter(new FileWriter(new File(measureSubtypesFile.getAbsolutePath() + "_MeasureSubTypes"), false));
				for (String measureSubtype : e.getValue())
					w.write(measureSubtype + "\t");
				
				w.close();
			}
			
			/* Output columns */
			br = FileUtil.getFileReader(summaryRotationSourceFile.getAbsolutePath());
			line = null;
			while ((line = br.readLine()) != null) {
				Map<String, Double> values = new TreeMap<String, Double>();
				Pair<CorpNetSummaryEntry, File> info = getInfoForRotationLine(line, outputDir, values);
				File outputFile = info.second();
				CorpNetSummaryEntry entry = info.first();
				
				Set<String> columns = fileMeasureSubtypes.get(outputFile.getAbsolutePath());
				BufferedWriter w = new BufferedWriter(new FileWriter(outputFile, true));
				w.write(entry.getObjectId() + "\t");
				for (String column : columns) {
					if (values.containsKey(column))
						w.write(values.get(column) + "\t");
					else
						w.write(0.0 + "\t");
				}
				w.write("\n");
				w.close();
			}
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static Pair<CorpNetSummaryEntry, File> getInfoForRotationLine(String line, File outputDir, Map<String, Double> outValues) {
		String[] lineParts = line.split("\t");
		CorpNetSummaryEntry entry = CorpNetSummaryEntry.fromString(lineParts[0]);
		Map<String, Double> values = JSONUtil.objToDistribution(JSONObject.fromObject(lineParts[1]));
		String net = entry.getFilterStr().split("/")[1];
	
		File lineOutputDir = new File(outputDir.getAbsolutePath(), net + "/" + entry.getMeasure().getName());
		if (!lineOutputDir.exists() && !lineOutputDir.mkdirs()) {
			System.out.println("Failed to create output directory: " + lineOutputDir.getAbsolutePath() + "... exiting.");
		}
		
		File outputFile = new File(lineOutputDir.getAbsoluteFile(), entry.getObjectType().toString());
		
		for (Entry<String, Double> e : values.entrySet())
			outValues.put(e.getKey(), e.getValue());
		
		return new Pair<CorpNetSummaryEntry, File>(entry, outputFile);
	}
	
	public static void outputSummaryAggregation(File outputDir, String source) {
		File summaryAggregationSourceFile = new File(properties.getNetworkSourceDirPath(), source + "_SummaryAggregation");
	
		try {
			BufferedReader br = FileUtil.getFileReader(summaryAggregationSourceFile.getAbsolutePath());
			String line = null;
			
			while ((line = br.readLine()) != null) {
				CorpNetSummaryEntry entry = CorpNetSummaryEntry.fromString(line);
				String net = entry.getFilterStr().split("/")[1];
				
				File lineOutputDir = new File(outputDir.getAbsolutePath(), net + "/" + entry.getMeasure().getName());
				if (!lineOutputDir.exists() && !lineOutputDir.mkdirs()) {
					System.out.println("Failed to create output directory: " + lineOutputDir.getAbsolutePath() + "... exiting.");
					return;
				}
				
				File outputFile = new File(lineOutputDir.getAbsoluteFile(), 
											entry.getObjectType().toString()
											+ "_" + entry.getAggType() 
											+ ((entry.getAggType() == CorpNetSummaryEntry.AggregationType.HISTOGRAM) ? "_" + entry.getMeasureSubType() : "" )
										);
				
				if (!outputFile.getParentFile().exists() && !outputFile.getParentFile().mkdirs()) {
					System.out.println("Failed to create output directory: " + outputFile.getParentFile().getAbsolutePath() + "... exiting.");
					return;
				}
				
				BufferedWriter w = new BufferedWriter(new FileWriter(outputFile, true));

				if (entry.getAggType() == CorpNetSummaryEntry.AggregationType.HISTOGRAM) {
					w.write(entry.getObjectId() + "\t" + entry.getValue() + "\n");
				} else {
					w.write(entry.getMeasureSubType() + "\t" + entry.getValue() + "\n");
				}
				
				w.close();
			}
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
