package corp.net.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import corp.net.summary.CorpNetMeasure;

public class HSummarizeCorpNet {	
	private static List<CorpNetFilter> initFilters() {
		List<CorpNetFilter> filters = new ArrayList<CorpNetFilter>();
		
		/* FIXME */
		
		return filters;
	}
		
	private static List<CorpNetMeasure> initMeasures() {
		List<CorpNetMeasure> measures = new ArrayList<CorpNetMeasure>();
		
		/* FIXME */
		
		return measures;
	}
	
	
	public static class HSummarizeCorpNetMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private Text key = new Text();
		private DoubleWritable value = new DoubleWritable();
		
		public void run(Context context) throws InterruptedException {
			try {
				setup(context);
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
				}
				cleanup(context);
			} catch (Exception e) {

			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			List<CorpNetFilter> filters = initFilters();
			List<CorpNetMeasure> measures = initMeasures();
			
			String serializedObj = value.toString();
			CorpNetObject netObj = CorpNetObject.fromString(serializedObj);
			
			for (CorpNetFilter filter :  filters) {
				List<String> filterStrs = filter.filterObject(netObj);
				if (filterStrs == null)
					continue;
				for (CorpNetMeasure measure : measures) {
					Map<String, Double> measureValues = measure.map(netObj);
					for (String filterStr : filterStrs) {
						for (Entry<String, Double> measureEntry : measureValues.entrySet()) {
							this.key.set(filter.getName() + "." + filterStr + "." + measure.getName() + "." + measureEntry.getKey());
							this.value.set(measureEntry.getValue());
							context.write(this.key, this.value);
						}
					}	
				}
			}
		}
	}

	public static class HSummarizeCorpNetReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable value = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			String keyStr = key.toString();
			String[] keyParts = keyStr.split("\\.");
			String measureName = keyParts[2];
			CorpNetMeasure measure = CorpNetMeasure.fromString(measureName);
			this.value.set(measure.reduce(values));
			context.write(key, this.value);
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
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


