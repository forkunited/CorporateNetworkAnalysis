package corp.net.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import corp.net.summary.CorpNetSummaryEntry;
import corp.net.util.JSONUtil;

public class HRotateCorpNetSummary {
	public static class HRotateCorpNetSummaryMapper extends Mapper<Object, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			CorpNetSummaryEntry inputEntry = CorpNetSummaryEntry.fromString(value.toString());

			CorpNetSummaryEntry outputEntry = inputEntry.clone();
			outputEntry.setMeasureSubType("");
			
			JSONObject outputValue = new JSONObject();
			outputValue.put(inputEntry.getMeasureSubType(), inputEntry.getValue());
			
			this.key.set(outputEntry.getKey());
			this.value.set(outputValue.toString());
			context.write(this.key, this.value);
		}
	}

	public static class HRotateCorpNetSummaryReducer extends Reducer<Text, Text, Text, Text> {
		private Text value = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			JSONObject measureValues = new JSONObject();
			for (Text value : values) {
				JSONObject measureValueObj = JSONObject.fromObject(value.toString());
				Map<String, Double> measureValue = JSONUtil.objToDistribution(measureValueObj);
				for (Entry<String, Double> e : measureValue.entrySet())
					measureValues.put(e.getKey(), e.getValue());
			}
			this.value.set(measureValues.toString());
			context.write(key, this.value);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HRotateCorpNetSummary");
		job.setJarByClass(HRotateCorpNetSummary.class);
		job.setMapperClass(HRotateCorpNetSummaryMapper.class);
		job.setReducerClass(HRotateCorpNetSummaryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
