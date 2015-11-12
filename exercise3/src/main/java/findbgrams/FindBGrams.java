package findbgrams;

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

public class FindBGrams
{
	public static class NewMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final Text text = new Text();
		private final IntWritable num = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] words = value.toString().split("(?<!\\G\\w+)\\s");
			// String[] words = value.toString().split("\\s");
			for(String w : words){
				if (w.matches("\\w+ \\w+")){
					text.set(w);
					context.write(text, num);
				}
			}
		}
	}

	public static class NewReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private int max = 0;
		private String maxBgram = null;

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int res = 0;
			for (IntWritable val : values) {
				res += val.get();
			}

			if (res>max){
				max = res;
				maxBgram = key.toString();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(maxBgram), new IntWritable(max));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FindBGrams");
		job.setJarByClass(FindBGrams.class);

		// // Set map key and value classes
    // job.setMapOutputKeyClass(LongWritable.class);
    // job.setMapOutputValueClass(Text.class);

    // Set reduce key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(NewMapper.class);
		job.setReducerClass(NewReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
