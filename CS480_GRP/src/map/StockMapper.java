package map;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Output formats: 
 *
 */
public class StockMapper extends Mapper<Text, BytesWritable, Text, Text> {

	private static Text word = new Text();
	private static Text output = new Text();

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

		// This is the text file(s) in this input split, probably not going to need this
		System.out.println(key);
		
		// Get the value of the text file, this is what we're after
		word.set(value.getBytes());
		System.out.println(word.toString());

	}// END map

}
