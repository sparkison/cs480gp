package combinefiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import org.apache.hadoop.util.ToolRunner;

public class FileLineWritable extends Configured implements Tool {
	
	static final String usage = "Please use format: \"util.FileCombiner [input_path] [output_path] [number_of_files]\"";
	
	static class SequenceFileMapper extends
	Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filename;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filename = new Text(path.toString());
		}

		@Override
		protected void map(NullWritable key, BytesWritable value,
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

		// Remove old output paths, if exist
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FileLineWritable.class);
		job.setJobName("Combine small files");
		job.setInputFormatClass(CFInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setNumReduceTasks(numFiles);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(SequenceFileMapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FileLineWritable(), args);
		System.exit(exitCode);
	}
}
