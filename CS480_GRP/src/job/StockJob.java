package job;

import map.StockMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import reduce.StockReducer;

public class StockJob extends Configured implements Tool {

	static final String usage = "Please use format: \"driver.StockDriver [input_path] [output_path]\"";
	
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

		// Remove old output path, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		// Setup the job...
		Job job = Job.getInstance(conf, "Stock analysis");
		// Make sure input format set for SequenceFiles
		job.setInputFormatClass(SequenceFileInputFormat.class);
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
		return job.waitForCompletion(true) ? 0 : 1;
	}

}