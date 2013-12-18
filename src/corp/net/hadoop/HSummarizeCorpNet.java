package corp.net.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import corp.net.CorpNetObject;
import corp.net.summary.CorpNetFilter;
import corp.net.summary.CorpNetFilterNet;
import corp.net.summary.CorpNetMeasure;
import corp.net.summary.CorpNetMeasureDegreeIn;
import corp.net.summary.CorpNetMeasureDegreeOut;
import corp.net.summary.CorpNetMeasureDegreeReturn;
import corp.net.summary.CorpNetMeasureDegreeTotal;
import corp.net.summary.CorpNetMeasureMentionCount;
import corp.net.summary.CorpNetMeasurePSum;
import corp.net.summary.CorpNetSummaryEntry;

public class HSummarizeCorpNet {	
	private static List<CorpNetFilter> initFilters() {
		List<CorpNetFilter> filters = new ArrayList<CorpNetFilter>();
		
		filters.add(new CorpNetFilterNet("FULL"));
		filters.add(new CorpNetFilterNet("1994"));
		filters.add(new CorpNetFilterNet("1995"));
		filters.add(new CorpNetFilterNet("1996"));
		filters.add(new CorpNetFilterNet("1997"));
		filters.add(new CorpNetFilterNet("1998"));
		filters.add(new CorpNetFilterNet("1999"));
		filters.add(new CorpNetFilterNet("2000"));
		filters.add(new CorpNetFilterNet("2001"));
		filters.add(new CorpNetFilterNet("2002"));
		filters.add(new CorpNetFilterNet("2003"));
		filters.add(new CorpNetFilterNet("2004"));
		filters.add(new CorpNetFilterNet("2005"));
		filters.add(new CorpNetFilterNet("2006"));
		filters.add(new CorpNetFilterNet("2007"));
		filters.add(new CorpNetFilterNet("2008"));
		filters.add(new CorpNetFilterNet("2009"));
		filters.add(new CorpNetFilterNet("2010"));
		filters.add(new CorpNetFilterNet("2011"));
		filters.add(new CorpNetFilterNet("2012"));
		
		return filters;
	}
		
	private static List<CorpNetMeasure> initMeasures() {
		List<CorpNetMeasure> measures = new ArrayList<CorpNetMeasure>();
		measures.add(new CorpNetMeasureDegreeIn());
		measures.add(new CorpNetMeasureDegreeOut());
		measures.add(new CorpNetMeasureDegreeReturn());
		measures.add(new CorpNetMeasureDegreeTotal());
		measures.add(new CorpNetMeasureMentionCount());
		measures.add(new CorpNetMeasurePSum());
		
		return measures;
	}
	
	
	public static class HSummarizeCorpNetMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private Text key = new Text();
		private DoubleWritable value = new DoubleWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<CorpNetFilter> filters = initFilters();
			List<CorpNetMeasure> measures = initMeasures();
			
			String serializedObj = value.toString();
			CorpNetObject netObj = CorpNetObject.fromString(serializedObj);
			
			for (CorpNetFilter filter :  filters) {
				if (!filter.filterObject(netObj))
					continue;
				for (CorpNetMeasure measure : measures) {
					CorpNetSummaryEntry entry = new CorpNetSummaryEntry(filter.toString(), measure);
					List<CorpNetSummaryEntry> measureValues = measure.map(entry, netObj);
					if (measureValues == null)
						continue;
					for (CorpNetSummaryEntry measureEntry : measureValues) {
						this.key.set(measureEntry.getKey());
						this.value.set(measureEntry.getValue());
						context.write(this.key, this.value);
					}
				}
			}
		}
	}

	public static class HSummarizeCorpNetReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable value = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			CorpNetSummaryEntry entry = CorpNetSummaryEntry.fromString(key.toString());
			Double reduceValue = entry.getMeasure().reduce(values);
			if (reduceValue != null) {
				this.value.set(reduceValue);
				context.write(key, this.value);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HSummarizeCorpNet");
		job.setJarByClass(HSummarizeCorpNet.class);
		job.setMapperClass(HSummarizeCorpNetMapper.class);
		job.setReducerClass(HSummarizeCorpNetReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


