package map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import analysis.DayStatsWritable;

/*
 * Output formats: 
 *
 */
public class StockMapper extends Mapper<Text, BytesWritable, Text, Text> {

	private static Text line = new Text();
	private static Text output = new Text();
	private static DayStatsWritable day;

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

		// This is the text file in this input split, probably not going to need this
		System.out.println(key);
		
		// Get the value of the text file, this is what we're after
		line.set(value.getBytes());
		StringTokenizer token = new StringTokenizer(line.toString(), "\n");
		while (token.hasMoreTokens()) {
			try{
			day = new DayStatsWritable(new Text(token.nextToken()));
			}catch(Exception e){
				e.printStackTrace();
				
				System.exit(0);
			}
			System.out.println(day.toString());
		}
		
	}// END map

}
