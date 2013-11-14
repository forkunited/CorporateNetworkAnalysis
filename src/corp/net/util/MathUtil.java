package corp.net.util;

import java.util.Map;
import java.util.Map.Entry;

public class MathUtil {
	public static void accumulateDistribution(Map<String, Double> distribution, Map<String, Double> delta) {
		for (Entry<String, Double> entry : delta.entrySet()) {
			if (!distribution.containsKey(entry.getKey()))
				distribution.put(entry.getKey(), 0.0);
			distribution.put(entry.getKey(), distribution.get(entry.getKey()) + entry.getValue());
		}
	}

	public static void accumulateHistogram(Map<String, Integer> histogram, Map<String, Integer> delta) {
		for (Entry<String, Integer> entry : delta.entrySet()) {
			if (!histogram.containsKey(entry.getKey()))
				histogram.put(entry.getKey(), 0);
			histogram.put(entry.getKey(), histogram.get(entry.getKey()) + entry.getValue());
		}
	}
	
	public static String argMaxDistribution(Map<String, Double> distribution) {
		String maxArg = null;
		double max = 0;
		for (Entry<String, Double> entry : distribution.entrySet()) {
			if (entry.getValue() > max) {
				max = entry.getValue();
				maxArg = entry.getKey();
			}
		}
		return maxArg;
	}
}
