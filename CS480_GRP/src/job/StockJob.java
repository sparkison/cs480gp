package job;

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

import map.EMAMapper;
import map.HiLowMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;

import reduce.EMAReducer;
import reduce.HiLowReducer;
import writable.DayStatsWritable;

public class StockJob extends Configured implements Tool {
	
	static final String usage = "Please use format: \"driver.StockJob [input_path] [output_path]\"";

	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length < 2){
			System.out.println(usage);
			System.exit(1);
		}

		String inputPath = args[0];
		String outputPath = args[1];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);
		
		Path hiLowOut = new Path(outPath.getName()+"_hiLow");
		Path emaOut = new Path(outPath.getName()+"_emas");

		// Remove old output path, if exist
		if (fs.exists(hiLowOut)) {
			fs.delete(hiLowOut, true);
		}
		if (fs.exists(emaOut)) {
			fs.delete(emaOut, true);
		}
		
		Job job = Job.getInstance(conf, "Hi/Lows");
		
		// Make sure input format set for SequenceFiles
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setJarByClass(StockJob.class);
		
		job.setMapperClass(HiLowMapper.class);
		
		job.setReducerClass(HiLowReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DayStatsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, hiLowOut);
		
		job.waitForCompletion(true); 
		
		Configuration conf2 = new Configuration(); 
		Job job2 = Job.getInstance(conf2, "EMAs");
		
		// Make sure input format set for SequenceFiles
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		
		job2.setJarByClass(StockJob.class);
		
		job2.setMapperClass(EMAMapper.class);
		
		job2.setReducerClass(EMAReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DayStatsWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, inPath);
		FileOutputFormat.setOutputPath(job2, emaOut);
		
		return job2.waitForCompletion(true) ? 0 : 1;
		
	}
}