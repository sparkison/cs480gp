package driver;

import job.LineWritableJob;

import org.apache.hadoop.util.ToolRunner;

public class FileCombiner{

	/**
	 * Simple runner program
	 * args[0] = input
	 * args[1] = output
	 * args[2] = number of output files
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LineWritableJob(), args);
		System.exit(exitCode);
	}

}
