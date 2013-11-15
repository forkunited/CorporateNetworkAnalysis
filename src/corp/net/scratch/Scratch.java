package corp.net.scratch;

import java.io.BufferedReader;
import java.io.IOException;

import corp.net.CorpNetObject;

import ark.util.FileUtil;

public class Scratch {
	public static void main(String args[]) throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/sloan/Data/Test/testNetworkData.txt");
		String line = null;
		while ((line = r.readLine()) != null) {
			CorpNetObject object = CorpNetObject.fromString(line);
			System.out.println(object.toString());
		}
	}
}
