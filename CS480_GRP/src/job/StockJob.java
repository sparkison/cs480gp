package job;

import java.io.IOException;

import map.StockMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
        
        FileStatus[] inputPathFileList = fs.listStatus(inPath);
		final int StockDocCount = inputPathFileList.length;
		String[] fileNames = new String[StockDocCount];
		String[] fileSize = new String[StockDocCount];
		
		for (int i = 0; i<StockDocCount; i++) {
			// Get the filename
			fileNames[i] = inputPathFileList[i].getPath().getName();
			// Determine the length of the file
			Path path = new Path(fileNames[i]);
	        FileSystem hdfs = path.getFileSystem(conf);
	        ContentSummary cSummary = hdfs.getContentSummary(path);
	        // Write length to array
	        Long size = new Long(cSummary.getLength());
			fileSize[i] = size.toString();
		}
		/* Write the arrays to the configuration for easy retrieval
		 * 
		 * Get filename using: context.getConfiguration().getStrings("documentNames");
		 * Get filesize using: context.getConfiguration().getStrings("documentSizes");
		 * 
		 * More info here: 
		 * https://hadoop.apache.org/docs/r2.2.0/api/org/apache/hadoop/conf/Configuration.html#getStrings(java.lang.String)
		 */
		conf.setStrings("documentNames", fileNames);
		conf.setStrings("documentSizes", fileSize);
		
		// Setup the job...
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