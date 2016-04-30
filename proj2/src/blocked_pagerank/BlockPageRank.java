package blocked_pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BlockPageRank {

	public static final String DELIMITER = "$";
	public static final String DELIMITER_REGEX = "\\$";
	public static final String PAGE_RANK = "PR";
	public static final String EDGES_FROM_BLOCK = "BE";
	public static final String BOUNDARY_CONDITION = "BC";
	public static final Double RESIDUAL_ERROR_THRESHOLD = 0.001;
	public static final Integer NUM_BLOCKS = 68;
	public static final Integer COUNTER_FACTOR = 100000000;
	
	public static void main(String[] args) throws Exception {
		
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
			Job bprJob = Job.getInstance(conf, "blocked_pagerank");
			bprJob.setJobName("BPR" + passCount);
			bprJob.setJarByClass(blocked_pagerank.BlockPageRank.class);
			
			// Setting the mapper and reducer for blocked page rank
			bprJob.setMapperClass(BPRMapper.class);
			bprJob.setReducerClass(BPRReducer.class);
			bprJob.setOutputKeyClass(LongWritable.class);
			bprJob.setOutputValueClass(Text.class);

			// Setting the input path based on the pass count
			inputPath = passCount == 0 ? args[1]:args[2] + passCount;
			FileInputFormat.addInputPath(bprJob, new Path(inputPath));
			System.out.println("Setting input for pass count " + passCount + " as "+ inputPath);
			
			// Setting the output path based on the pass count
			outputPath = args[2] + (passCount+1);
			FileOutputFormat.setOutputPath(bprJob, new Path(outputPath));
			System.out.println("Setting output for pass count " + passCount + " as " + outputPath);
			
			// Waiting for the map reduce job to complete
			bprJob.waitForCompletion(true);
			
			// Get the current residual error
			residualError = (double) bprJob.getCounters().findCounter(BPRCounter.RESIDUAL_ERROR).getValue();
			System.out.println("Residual error after map reduce iteration is: "+residualError);
			
			// Convert residual error back to double and divide by number of blocks to get average
			actualResidualError = residualError / COUNTER_FACTOR;
			System.out.println("Actual residual error with counter factor: "+actualResidualError);
			
			averageResidualError = actualResidualError / NUM_BLOCKS;
			System.out.println("Average residual error with num blocks: "+averageResidualError);
			
			System.out.println("Final Residual error for iteration " + passCount 
					+ ": " + String.format("%.6f", averageResidualError));

			// Reset residual error counter
		    Counters counters = bprJob.getCounters();
		    Counter c1 = counters.findCounter(BPRCounter.RESIDUAL_ERROR);
			c1.setValue(0L);
			passCount++;
			
			System.out.println("------------------------------------------------------------------------");
		} while(averageResidualError > RESIDUAL_ERROR_THRESHOLD);
	}
}
