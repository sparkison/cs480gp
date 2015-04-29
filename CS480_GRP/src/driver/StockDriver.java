package driver;

import org.apache.hadoop.util.ToolRunner;

import job.StockJob;

public class StockDriver {

	/**
	 * Simple runner program
	 * args[0] = input (the output directory from the 'FileCombiner' class)
	 * args[1] = output
	 */
	public static void main(String args[]) throws Exception{
		int exitCode = ToolRunner.run(new StockJob(), args);
		System.exit(exitCode);
	}

}
