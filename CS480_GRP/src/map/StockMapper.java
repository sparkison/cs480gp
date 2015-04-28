package map;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Output formats: 
 *
 */
public class StockMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Text word = new Text();
	private static Text output = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



	}// END map

}
