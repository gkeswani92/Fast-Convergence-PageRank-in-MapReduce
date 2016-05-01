package gaussseidel_pagerank;

import java.util.ArrayList;
import java.util.List;

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

		System.out.println("Running Gauss Seidel Page Rank");
		Double residualError = 0.0;
		Double averageResidualError = 0.0;
		Double actualResidualError = 0.0;
		Integer passCount = 0;
		String inputPath = "";
		String outputPath = "";
		
		// Map Reduce configuration
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ";");
		Job gaussSeidelJob = null;
		
		// Keeping track of number of iterations per block over all passes
		List<Long> blockIterations = new ArrayList<Long>();
		Long totalAvgIterations = 0L;	
		for(int i=0; i<68; i++){
			blockIterations.add(0l);
		}

		do {
			System.out.println("Current Pass Count: " + passCount);
			gaussSeidelJob = Job.getInstance(conf, "jacobi_pagerank");
			gaussSeidelJob.setJobName("Gauss Seidel " + passCount);
			gaussSeidelJob.setJarByClass(gaussseidel_pagerank.GaussSeidelPageRank.class);

			// Setting the mapper and reducer for blocked page rank
			gaussSeidelJob.setMapperClass(BPRMapper.class);
			gaussSeidelJob.setReducerClass(GaussSeidelReducer.class);
			gaussSeidelJob.setOutputKeyClass(LongWritable.class);
			gaussSeidelJob.setOutputValueClass(Text.class);

			// Setting the input path based on the pass count
			inputPath = passCount == 0 ? args[1] : args[2] + passCount;
			FileInputFormat.addInputPath(gaussSeidelJob, new Path(inputPath));
			System.out.println("Setting input for pass count " + passCount + " as " + inputPath);

			// Setting the output path based on the pass count
			outputPath = args[2] + (passCount + 1);
			FileOutputFormat.setOutputPath(gaussSeidelJob, new Path(outputPath));
			System.out.println("Setting output for pass count " + passCount + " as " + outputPath);

			// Waiting for the map reduce job to complete
			gaussSeidelJob.waitForCompletion(true);

			// Get the current residual error
			residualError = (double) gaussSeidelJob.getCounters().findCounter(CustomCounter.RESIDUAL_ERROR).getValue();
			System.out.println("Residual error after map reduce iteration is: " + residualError);

			// Convert residual error back to double and divide by number of
			// blocks to get average
			actualResidualError = residualError / Constants.COUNTER_FACTOR;
			//System.out.println("Actual residual error with counter factor: " + actualResidualError);

			averageResidualError = actualResidualError / Constants.NUM_BLOCKS;
			//System.out.println("Average residual error with num blocks: " + averageResidualError);

			System.out.println("Final Residual error for iteration " + passCount + ": "
					+ String.format("%.6f", averageResidualError));

			// Reset residual error counter
			Counters counters = gaussSeidelJob.getCounters();
			Counter c1 = counters.findCounter(CustomCounter.RESIDUAL_ERROR);
			c1.setValue(0L);
			passCount++;
			
			// Adding the number of iterations of each block in the current map reduce pass
			for (int i = 0 ; i < Constants.NUM_BLOCKS; i++) {
				String counterName = "ITERATIONS_BLOCK_" + (i+1);
				Counter counter = gaussSeidelJob.getCounters().findCounter(CustomCounter.valueOf(counterName));
				blockIterations.set(i, blockIterations.get(i) + counter.getValue());
			}
			System.out.println("------------------------------------------------------------------------");
		} while (averageResidualError > Constants.RESIDUAL_ERROR_THRESHOLD);
		
		// Displaying iterations per block and then average of all blocks
		for (int i = 0; i < Constants.NUM_BLOCKS; i++) {
			System.out.println("Average of Block " + i + ": " + String.format("%.3f", (blockIterations.get(i)*1.00/passCount)) + " iterations");
			totalAvgIterations += blockIterations.get(i);
		}
		System.out.println("Total average iterations for all blocks using Gauss Seidel: " + ((totalAvgIterations*1.0000)/(68*passCount)));
	}
}
