package allcaps;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AllCaps
{
	public static class NewMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			value.set(value.toString().toUpperCase());
			context.write(key, value);
		}
	}

	public static class NewReducer extends Reducer<LongWritable, Text, Text, NullWritable>
	{
		private Text text = new Text();

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String res = "";
			for (Text val : values) {
				res += val.toString();
			}
			text.set(res);
			context.write(text, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "allcaps");
		job.setJarByClass(AllCaps.class);

		// Set map key and value classes
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    // Set reduce key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
