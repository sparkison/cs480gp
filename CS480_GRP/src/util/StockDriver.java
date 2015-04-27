package util;

import java.io.IOException;

import job.StockJob;

public class StockDriver {

	public static void main(String args[]) {
		
		if(args.length < 2 || args == null) {
			System.out.println("Incorrect number of arguments used.\nPlease use: \"util.StockDriver [input_path] [output_path]\"");
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		int status = -1;
		
		// The census job runner
		StockJob stockJob = new StockJob(input, output);
		try {
			/*
			 * Start the MR task
			 */
			status = stockJob.start();
		} catch (IllegalArgumentException | ClassNotFoundException
				| IOException | InterruptedException e) {
			System.out.println("Error starting Stock map reduce job: ");
			e.printStackTrace();
			System.exit(status);
		}
		
	}
	
}
