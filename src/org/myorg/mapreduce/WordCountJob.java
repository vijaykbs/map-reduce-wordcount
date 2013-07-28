package org.myorg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountJob {

	public static void main(String args[]) throws IOException{
		JobConf job = new JobConf(WordCountJob.class);
		job.setJobName("Word Count Example");

		FileInputFormat.setInputPaths(job, args[0]);
		job.setInputFormat(TextInputFormat.class);

		job.setMapperClass(MapTask.class);
		job.setCombinerClass(ReduceTask.class);
		job.setReducerClass(ReduceTask.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);
	} 
}