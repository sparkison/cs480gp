package map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import writable.CompositeKey;
import writable.DayStatsWritable;

public class HiLowMapper extends Mapper<Object, Text, CompositeKey, DayStatsWritable>{

	private static CompositeKey tickerDate = new CompositeKey();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		DayStatsWritable dayStat;

		//		DayStatsWritable valout = new DayStatsWritable(value);
		//		mapkey.set(valout.getTicker());
		//		context.write(mapkey, valout);

		BufferedReader bufReader = new BufferedReader(new StringReader(value.toString()));
		String line = null;
		Text ticker = new Text();
		Text date = new Text();
		while( (line = bufReader.readLine()) != null ) {

			try{
				dayStat = new DayStatsWritable(new Text(line));
				ticker.set(dayStat.getTicker());
				date.set(dayStat.getDate());
				if (!(ticker == null || date == null)){
					tickerDate.setTicker(ticker);
					tickerDate.setDate(date);
					context.write(tickerDate, dayStat);
				}
			}catch(Exception e){
				System.out.println("\n\n" + line + "\n\n");
				e.printStackTrace();
				System.exit(0);
			}

		}

	}
}