package gaussseidel_pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.CustomCounter;
import utils.Constants;
import blocked_pagerank.BPRMapper;

public class GaussSeidelPageRank {
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("Running Jacobi Page Rank");
		Double residualError = 0.0;
		Double averageResidualError = 0.0;
		Double actualResidualError = 0.0;
		Integer passCount = 0;
		String inputPath = "";
		String outputPath = "";
		
		do {
			
			Configuration conf = new Configuration();
			conf.set("mapreduce.output.textoutputformat.separator", ";");
			
			System.out.println("Current Pass Count: " + passCount);
			Job jacobiJob = Job.getInstance(conf, "jacobi_pagerank");
			jacobiJob.setJobName("Jacobi" + passCount);
			jacobiJob.setJarByClass(gaussseidel_pagerank.GaussSeidelPageRank.class);
			
			// Setting the mapper and reducer for blocked page rank
			jacobiJob.setMapperClass(BPRMapper.class);
			jacobiJob.setReducerClass(GaussSeidelReducer.class);
			jacobiJob.setOutputKeyClass(LongWritable.class);
			jacobiJob.setOutputValueClass(Text.class);

			// Setting the input path based on the pass count
			inputPath = passCount == 0 ? args[1]:args[2] + passCount;
			FileInputFormat.addInputPath(jacobiJob, new Path(inputPath));
			System.out.println("Setting input for pass count " + passCount + " as "+ inputPath);
			
			// Setting the output path based on the pass count
			outputPath = args[2] + (passCount+1);
			FileOutputFormat.setOutputPath(jacobiJob, new Path(outputPath));
			System.out.println("Setting output for pass count " + passCount + " as " + outputPath);
			
			// Waiting for the map reduce job to complete
			jacobiJob.waitForCompletion(true);
			
			// Get the current residual error
			residualError = (double) jacobiJob.getCounters().findCounter(CustomCounter.RESIDUAL_ERROR).getValue();
			System.out.println("Residual error after map reduce iteration is: "+residualError);
			
			// Convert residual error back to double and divide by number of blocks to get average
			actualResidualError = residualError / Constants.COUNTER_FACTOR;
			System.out.println("Actual residual error with counter factor: "+actualResidualError);
			
			averageResidualError = actualResidualError / Constants.NUM_BLOCKS;
			System.out.println("Average residual error with num blocks: "+averageResidualError);
			
			System.out.println("Final Residual error for iteration " + passCount 
					+ ": " + String.format("%.6f", averageResidualError));

			// Reset residual error counter
		    Counters counters = jacobiJob.getCounters();
		    Counter c1 = counters.findCounter(CustomCounter.RESIDUAL_ERROR);
			c1.setValue(0L);
			passCount++;
			
			System.out.println("------------------------------------------------------------------------");
		} while(averageResidualError > Constants.RESIDUAL_ERROR_THRESHOLD);
	}
}
