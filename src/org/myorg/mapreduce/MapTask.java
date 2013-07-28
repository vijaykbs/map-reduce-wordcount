package org.myorg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapTask extends MapReduceBase implements 
								Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable inKey, Text inVal, OutputCollector<Text, 
				IntWritable> output, Reporter reporter) throws IOException {
		String line = inVal.toString();
		Text word = new Text();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			output.collect(word, new IntWritable(1));
		}
	}
}
