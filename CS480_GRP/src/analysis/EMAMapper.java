package analysis;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import analysis.DayStatsWritable;

public class EMAMapper extends Mapper<Object, BytesWritable, Text, DayStatsWritable>{

	private static Text line = new Text();
	private static Text mapkey = new Text();

	public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {

		DayStatsWritable dayStat;

		//		DayStatsWritable valout = new DayStatsWritable(value);
		//		mapkey.set(valout.getTicker());
		//		context.write(mapkey, valout);
		
		line.set(value.getBytes());
		StringTokenizer token = new StringTokenizer(line.toString(), "\n");
		while (token.hasMoreTokens()) {
			String s = token.nextToken();
			
			try{
				dayStat = new DayStatsWritable(new Text(s));
				mapkey.set(dayStat.getTicker());
				context.write(mapkey, dayStat);
			}catch(Exception e){
				System.out.println("\n\n" + s + "\n\n");
				e.printStackTrace();
				System.exit(0);
			}
			
			
		}
	}
}