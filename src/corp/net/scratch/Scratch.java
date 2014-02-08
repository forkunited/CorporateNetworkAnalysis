package corp.net.scratch;

import java.io.BufferedReader;
import java.io.IOException;

import corp.net.CorpNetObject;
import corp.net.summary.CorpNetMeasureDegreeIn;
import corp.net.summary.CorpNetSummaryEntry;

import ark.util.FileUtil;

/**
 * Scratch provides space for trying out temporary snippets of code.  This is
 * useful for performing one-off tasks or briefly testing things out.
 * 
 * @author Bill McDowell
 */
public class Scratch {
	public static void main(String args[]) throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/sloan/Data/Test/testNetworkData.txt");
		String line = null;
		while ((line = r.readLine()) != null) {
			CorpNetObject object = CorpNetObject.fromString(line);
			System.out.println(object.toString());
		}
		
		CorpNetSummaryEntry entry = new CorpNetSummaryEntry("NET", new CorpNetMeasureDegreeIn());
		entry.setMeasureSubType("ALL");
		entry.setObjectType(CorpNetObject.Type.NODE);
		entry.setObjectId("asdf");
		entry.setValue(1.0);
		
		String entryStr = entry.toString();
		System.out.println(entry.toString());
		CorpNetSummaryEntry newEntry = CorpNetSummaryEntry.fromString(entryStr);
		System.out.println(newEntry.toString());
	}
}
