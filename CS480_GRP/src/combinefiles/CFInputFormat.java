package combinefiles;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CFInputFormat extends
		FileInputFormat<NullWritable, Text> {
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false; 
	}

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		RecordReader<NullWritable, Text> reader = new CFRecordReader();
		reader.initialize(split, context);
		return reader;
	}
}