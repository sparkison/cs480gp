package map; 

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import writable.DayStatsWritable;


public class HybridMapper extends Mapper<Object, Text, Text, DayStatsWritable>{
	
	Text mapkey = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//		DayStatsWritable valout = new DayStatsWritable(value);
//		mapkey.set(valout.getTicker());
//		context.write(mapkey, valout);

		DayStatsWritable dayStat;

		BufferedReader bufReader = new BufferedReader(new StringReader(value.toString()));
		String line = null;
		while( (line = bufReader.readLine()) != null ) {


			try{
				dayStat = new DayStatsWritable(new Text(line));
				mapkey = dayStat.getTicker();
				if (mapkey != null)
					context.write(new Text(dayStat.getTicker()), dayStat);
			}catch(Exception e){
				System.out.println("\n\n" + line + "\n\n");
				e.printStackTrace();
				System.exit(0);
			}

		}
	}
}