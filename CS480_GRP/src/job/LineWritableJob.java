package job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import combinefiles.CFInputFormat;

public class LineWritableJob extends Configured implements Tool {
	
	static final String usage = "Please use format: \"driver.FileCombiner [input_path] [output_path] [number_of_files]\"";
	
	static class SequenceFileMapper extends
	Mapper<NullWritable, Text, Text, Text> {
		private Text filename;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filename = new Text(path.toString());
		}

		@Override
		protected void map(NullWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			context.write(filename, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
				
		if (args.length < 3){
			System.out.println(usage);
			System.exit(1);
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		int numFiles = Integer.parseInt(args[2]);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(inputPath);
		Path outPath = new Path(outputPath);

		// Remove old output path, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(LineWritableJob.class);
		job.setJobName("Combine small files");
		job.setInputFormatClass(CFInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setNumReduceTasks(numFiles);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SequenceFileMapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
