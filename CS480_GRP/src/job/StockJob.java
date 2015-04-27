package job;

import java.io.IOException;

import map.StockMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import partition.StockPartitioner;
import reduce.StockReducer;


public class StockJob {

	private String inputPath;
	private String outputPath;

	public StockJob(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	public String getInputPath(){
		return new String(inputPath);
	}

	public String getOutputPath(){
		return new String(outputPath);
	}

	/*
	 * This is the job for the Stock analysis
	 */
	public int start() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);

		// Remove old output paths, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(conf, "Stock analysis");
		job.setJarByClass(StockJob.class);

		// Set Map, Partition, Combiner, and Reducer classes
		job.setMapperClass(StockMapper.class);
		// job.setPartitionerClass(StockPartitioner.class);
		// job.setNumReduceTasks(7);
		job.setReducerClass(StockReducer.class);

		// Set the Map output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// Set the Reduce output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the output paths for the job
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		// Block for job to complete...
		int status = job.waitForCompletion(true) ? 0 : 1;

		return status;

	}

}