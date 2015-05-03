package analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CleanUpReducer extends Reducer<Text,CleanUpWritable,Text,Text> { 
	
	public void reduce(Text key, Iterable<CleanUpWritable> values, Context context) throws IOException, InterruptedException{
		for(CleanUpWritable val: values){
			context.write(key, new Text(val.toString()));
		}
	}
}
