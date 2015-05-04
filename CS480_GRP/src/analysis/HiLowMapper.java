package analysis;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import analysis.DayStatsWritable;

public class HiLowMapper extends Mapper<Object, BytesWritable, Text, DayStatsWritable>{

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
			
			// Debugging
			if (s.trim().length() == 0){
				System.out.println("*********************    BREAKING    ******************************");
				break;
			}
						
			try{
				dayStat = new DayStatsWritable(new Text(s));
				mapkey = dayStat.getTicker();
				context.write(new Text(dayStat.getTicker()), dayStat);
			}catch(Exception e){
				System.out.println("\n\n" + s + "\n\n");
				e.printStackTrace();
				System.exit(0);
			}

		}
	}
}