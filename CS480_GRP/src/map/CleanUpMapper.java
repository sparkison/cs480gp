package map;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import writable.CleanUpWritable;

public class CleanUpMapper extends Mapper<Object, Text, Text, CleanUpWritable>{

//	Text mapkey = new Text();
//	Text valout = new Text(); 
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//		String[] parsed = value.toString().trim().split("\t");
//		mapkey.set(parsed[0].trim());
//		String stringer = ""; 
//		for(int i = 1; i < parsed.length; i++){
//			if(i == parsed.length-1){
//				stringer = stringer + parsed[i].trim();
//			}else 
//				stringer = stringer + parsed[i].trim()+"\t";
//		}
//		valout.set(stringer);
		
		CleanUpWritable valout = new CleanUpWritable(value);
		context.write(valout.getTicker(), valout);
	}
}