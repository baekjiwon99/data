import java.util.*;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20190967 {
	public static class MovieMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Text outputKey = new Text();
			IntWritable outputVal = new IntWritable(1);		
			
			String split[] = value.toString().split("::");
			String genres = split[2];

			StringTokenizer itr2 = new StringTokenizer(genres, "|");
			
			while(itr2.hasMoreTokens()) {
				outputKey.set(itr2.nextToken().trim());
				context.write(outputKey, outputVal);
			}
		}
	}
	
	public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			IntWritable output = new IntWritable();
			int sum = 0;
			
			for(IntWritable v : values) {
				sum += v.get();
			}
			
			output.set(sum);
			context.write(key, output);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.err.println("Usage: IMDB <in> <out>"); 
   			System.exit(2);
		}

		Job job = new Job(conf, "IMDBStudent20190967");
		
		job.setJarByClass(IMDBStudent20190967.class);
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
