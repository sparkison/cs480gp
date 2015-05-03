package analysis;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import util.DayStatsWritable;

public class HiLowMapper extends Mapper<Object, Text, Text, DayStatsWritable>{

	private static Text line = new Text();
	private static Text mapkey = new Text();
	private static DayStatsWritable dayStat;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		//		DayStatsWritable valout = new DayStatsWritable(value);
		//		mapkey.set(valout.getTicker());
		//		context.write(mapkey, valout);
		
		line.set(value.getBytes());
		StringTokenizer token = new StringTokenizer(line.toString(), "\n");
		while (token.hasMoreTokens()) {
			dayStat = new DayStatsWritable(new Text(token.nextToken()));
			// System.out.println(day.toString());
			mapkey.set(dayStat.getTicker());
			context.write(mapkey, dayStat);
		}
	}
}