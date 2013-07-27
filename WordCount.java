import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable inKey, Text inVal, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = inVal.toString();
			Text word = new Text();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, new IntWritable(1));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text inKey, Iterator <IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while(values.hasNext()) {
				sum = sum + values.next().get();			
			}
			output.collect(inKey, new IntWritable(sum));
		}
	}

	public static void main(String args[]) throws IOException{
		JobConf job = new JobConf(WordCount.class);
		job.setJobName("Word Count Example");

		FileInputFormat.setInputPaths(job, args[0]);
		job.setInputFormat(TextInputFormat.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);
	} 
}
