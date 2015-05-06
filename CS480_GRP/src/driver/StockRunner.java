package driver;

import job.StockJob;

import org.apache.hadoop.util.ToolRunner;

public class StockRunner{

	/**
	 * Simple runner program
	 * args[0] = input
	 * args[1] = output
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new StockJob(), args);
		System.exit(exitCode);
	}

}
