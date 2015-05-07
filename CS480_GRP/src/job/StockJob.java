package job;

/*
 * Author: Daniel Sullivan
 * Date: April 24, 2015
 * Term Project
 * Purpose: for Apache Pig, compare values in subsequent tuple fields
 * 
 */

import map.EMAMapper;
import map.HiLowMapper;
import map.HybridMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import reduce.EMAReducer;
import reduce.HiLowReducer;
import reduce.HybridReducer; 
import writable.CompositeKey;
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
		Path hybridOut = new Path(outPath.getName()+"_hybrid");

		// Remove old output path, if exist
		if (fs.exists(hiLowOut)) {
			fs.delete(hiLowOut, true);
		}
		if (fs.exists(emaOut)) {
			fs.delete(emaOut, true);
		}
		if (fs.exists(hybridOut)) {
			fs.delete(hybridOut, true);
		}
		
		Job job = Job.getInstance(conf, "Hi/Lows");
		
		// Make sure input format set for SequenceFiles
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setJarByClass(StockJob.class);
		
		job.setMapperClass(HiLowMapper.class);
		
		job.setReducerClass(HiLowReducer.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
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
		
		job2.setMapOutputKeyClass(CompositeKey.class);
		job2.setMapOutputValueClass(DayStatsWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, inPath);
		FileOutputFormat.setOutputPath(job2, emaOut);
		
		job2.waitForCompletion(true);

		Configuration conf3 = new Configuration(); 
		Job job3 = Job.getInstance(conf3, "HYBRID");
		
		// Make sure input format set for SequenceFiles
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		
		job3.setJarByClass(StockJob.class);
		
		job3.setMapperClass(HybridMapper.class);
		
		job3.setReducerClass(HybridReducer.class);
		
		job3.setMapOutputKeyClass(CompositeKey.class);
		job3.setMapOutputValueClass(DayStatsWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job3, inPath);
		FileOutputFormat.setOutputPath(job3, hybridOut);
		
		return job3.waitForCompletion(true) ? 0 : 1; 
		
	}
}
