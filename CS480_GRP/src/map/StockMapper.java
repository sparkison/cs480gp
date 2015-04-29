package map;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.DayStatsWritable;

/*
 * Output formats: 
 *
 */
public class StockMapper extends Mapper<Text, BytesWritable, Text, Text> {

	private static Text word = new Text();
	private static Text output = new Text();
	private static DayStatsWritable day;

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

		// This is the text file(s) in this input split, probably not going to need this
		System.out.println(key);
		
		// Get the value of the text file, this is what we're after
		day = new DayStatsWritable(new Text(value.getBytes()));
		System.out.println(day.toString());

	}// END map

}
