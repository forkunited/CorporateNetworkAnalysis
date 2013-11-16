package corp.net.hadoop;

import java.io.IOException;

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

import corp.net.summary.CorpNetSummaryEntry;

public class HAggregateCorpNetSummary {	
	public static class HAggregateCorpNetSummaryMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private Text key = new Text();
		private DoubleWritable value = new DoubleWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			CorpNetSummaryEntry inputEntry = CorpNetSummaryEntry.fromString(value.toString());
			
			CorpNetSummaryEntry histogramEntry = inputEntry.clone();
			histogramEntry.setObjectId(String.valueOf((int)Math.floor(inputEntry.getValue())));
			histogramEntry.setAggType(CorpNetSummaryEntry.AggregationType.HISTOGRAM);
			histogramEntry.setValue(1.0);
			
			CorpNetSummaryEntry sumEntry = inputEntry.clone();
			histogramEntry.setObjectId("");
			histogramEntry.setAggType(CorpNetSummaryEntry.AggregationType.SUM);			
			histogramEntry.setValue(inputEntry.getValue());
			
			CorpNetSummaryEntry countEntry = inputEntry.clone();
			histogramEntry.setObjectId("");
			histogramEntry.setAggType(CorpNetSummaryEntry.AggregationType.COUNT);			
			histogramEntry.setValue(1.0);		

			this.key.set(histogramEntry.getKey());
			this.value.set(histogramEntry.getValue());
			context.write(this.key, this.value);

			this.key.set(sumEntry.getKey());
			this.value.set(sumEntry.getValue());
			context.write(this.key, this.value);
			
			this.key.set(countEntry.getKey());
			this.value.set(countEntry.getValue());
			context.write(this.key, this.value);
		}
	}

	public static class HAggregateCorpNetSummaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable value = new DoubleWritable();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			this.value.set(sum);
			context.write(key, this.value);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HAggregateCorpNetSummary");
		job.setJarByClass(HAggregateCorpNetSummary.class);
		job.setMapperClass(HAggregateCorpNetSummaryMapper.class);
		job.setReducerClass(HAggregateCorpNetSummaryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}