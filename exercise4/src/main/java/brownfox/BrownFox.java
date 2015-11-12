package brownfox;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class BrownFox{

	public static class WordFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final Text word = new Text();
		private final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				str = str.toLowerCase();
				str = str.replaceAll("\\W","");
				if (str.matches("[^\\d,_]+")){
					word.set(fileName+"$"+str);
					context.write(word, one);
				}
			}
		}
	}

	public static class WordFrequencyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class WordCountMap extends Mapper<LongWritable, Text, Text, WritableMap>{
		private final Text word = new Text();
		private final Log log = LogFactory.getLog(WordCountMap.class);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] rowStr = value.toString().split("\t");
			log.error("ASçKLçKLASLçSKAçLAKSçLKAS");
			log.info("value: "+value.toString()+" "+rowStr[0] + " " + rowStr[1]);
			String[] keyStr = rowStr[0].split("\\$");
			log.info(keyStr.length);
			log.info("keyStr "+keyStr[0]+" "+keyStr[1]);
			word.set(keyStr[0]);
			WritableMap map = new WritableMap();
			map.put("n" , new IntWritable(Integer.parseInt(rowStr[1])));
			map.put("w", new Text(keyStr[1]));
			context.write(word, map);
		}
	}

	public static class WordCountReduce extends Reducer<Text, WritableMap, Text, Text>{
		private IntWritable sum = new IntWritable(0);
		private final Log log = LogFactory.getLog(WordCountMap.class);
		//private Text keyRes = new Text();
		public void reduce(Text key, Iterable<WritableMap> values, Context context) throws IOException, InterruptedException{
			sum.set(0);
			ArrayList<WritableMap> list = new ArrayList<WritableMap>();
			for (WritableMap val : values) {
				//log.info(sum.get());
				list.add(val);
				sum.set(sum.get() + val.<IntWritable>get("n").get() );
			}
			log.info(sum.get());

			for (WritableMap val : list) {
				log.info("second for");
				log.info(key + "$" + val.<Text>get("w") + "----"+ ""+val.<IntWritable>get("n") +"_"+ sum);
				context.write(new Text( key + "$" + val.<Text>get("w")), new Text( ""+val.<IntWritable>get("n") +"_"+ sum ));
			}
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();

		//WORD FREQUENCY JOB
		// Job job = new Job(conf, "BrownFoxFrequency");
		// job.setJarByClass(BrownFox.class);
    // // Set reduce key and value classes
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(IntWritable.class);
		// job.setMapperClass(WordFrequencyMap.class);
		// job.setReducerClass(WordFrequencyReduce.class);
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));


		//WORD COUNT JOB
		Job job2 = new Job(conf, "BrownFoxCounter");
		job2.setJarByClass(BrownFox.class);
		job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(WritableMap.class);
    // Set reduce key and value classes
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
		job2.setMapperClass(WordCountMap.class);
		job2.setReducerClass(WordCountReduce.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
