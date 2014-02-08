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

/**
 * HAggregateCorpNetSummary aggregates the values output by 
 * corp.net.hadoop.HSummarizeCorpNet into histograms, counts and sums for 
 * each filtered measure sub-type. Each line of output is of the
 * form:
 * 
 * <CorpnetSummaryEntry aggregate key>	<Value>
 * 
 * Where the aggregate key describes the filtered measure-subtype and
 * the aggregation, and the value is the value for that aggregation for the
 * measure. There is a separate aggregate key-value pair for each entry in a 
 * histogram of a measure, but there is only one aggregate key-value for a 
 * measure's count and one aggregate key-value for a measure's sum.  See
 * corp.net.summary.CorpNetSummaryEntry for more details on the key.
 * 
 * You can easily compute the mean for a measure by the output sum divided 
 * by count, and also normalize the histogram using count, but there 
 * currently isn't any code to do this automatically.
 * 
 * @author Bill McDowell
 *
 */
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
			sumEntry.setObjectId("");
			sumEntry.setAggType(CorpNetSummaryEntry.AggregationType.SUM);			
			sumEntry.setValue(inputEntry.getValue());
			
			CorpNetSummaryEntry countEntry = inputEntry.clone();
			countEntry.setObjectId("");
			countEntry.setAggType(CorpNetSummaryEntry.AggregationType.COUNT);			
			countEntry.setValue(1.0);		

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