package analysis;

/*
 * Author: Daniel Sullivan
 * Date: April 24, 2015
 * Term Project
 * Purpose: for Apache Pig, compare values in subsequent tuple fields
 * 
 */

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockRunner {

	public static void main(String[] args) throws Exception {
		
		
		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "Hi/Lows");
		
		// Make sure input format set for SequenceFiles
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setJarByClass(StockRunner.class);
		
		job.setMapperClass(HiLowMapper.class);
		
		job.setReducerClass(HiLowReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DayStatsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"hiLows"));
		
		job.waitForCompletion(true); 
		
		Configuration conf2 = new Configuration(); 
		Job job2 = Job.getInstance(conf2, "EMAs");
		
		// Make sure input format set for SequenceFiles
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		
		job2.setJarByClass(StockRunner.class);
		
		job2.setMapperClass(EMAMapper.class);
		
		job2.setReducerClass(EMAReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DayStatsWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"emas"));
		
		job2.waitForCompletion(true); 
		
	}
}