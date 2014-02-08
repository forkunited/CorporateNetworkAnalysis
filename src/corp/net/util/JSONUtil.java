package corp.net.util;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.sf.json.JSONObject;

/**
 * JSONUtil provides functions for converting JSON objects to and from other  
 * data-structures.
 * 
 * @author Bill McDowell
 *
 */
public class JSONUtil {
	@SuppressWarnings("rawtypes")
	public static Map<String, Double> objToDistribution(JSONObject obj) {
		Map<String, Double> dist = new TreeMap<String, Double>();
		Set entries = obj.entrySet();
		for (Object o : entries) {
			Entry e = (Entry)o;
			String label = e.getKey().toString();
			double value = Double.parseDouble(e.getValue().toString());
			dist.put(label, value);
		}
		
		return dist;
	}
	
	@SuppressWarnings("rawtypes")
	public static Map<String, Integer> objToHistogram(JSONObject obj) {
		Map<String, Integer> hist = new TreeMap<String, Integer>();
		Set entries = obj.entrySet();
		for (Object o : entries) {
			Entry e = (Entry)o;
			String label = e.getKey().toString();
			int value = Integer.parseInt(e.getValue().toString());
			hist.put(label, value);
		}
		
		return hist;
	}
}
