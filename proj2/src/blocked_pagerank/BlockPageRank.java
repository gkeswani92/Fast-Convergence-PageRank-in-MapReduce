package blocked_pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BlockPageRank {

	public static final String DELIMITER = "$";
	public static final String PAGE_RANK = "PR";
	public static final String EDGES_FROM_BLOCK = "BE";
	public static final String BOUNDARY_CONDITION = "BC";
	public static final Double RESIDUAL_ERROR_THRESHOLD = 0.001;
	public static final Integer NUM_BLOCKS = 68;
	public static final int COUNTER_FACTOR = 1000000;

	public static enum BPRCounters {
		RESIDUAL_ERROR
	};
	
	public static void main(String[] args) throws Exception {
		
//		String inputFile = "Data/input.txt";
//		String outputFile = "Data/output";
		
		Double residualError = 0.0;
		Integer passCount = 0;
		
		do {
			Job bprJob = new Job();
			bprJob.setJobName("BPR" + passCount++);
			bprJob.setJarByClass(blocked_pagerank.BlockPageRank.class);
			
			bprJob.setMapperClass(BPRMapper.class);
			bprJob.setReducerClass(BPRReducer.class);
			
			if (passCount == 0) {
				FileInputFormat.addInputPath(bprJob, new Path(args[0])); 	
			} else {
				FileInputFormat.addInputPath(bprJob, new Path(args[1] + passCount)); 
			}
			
			FileOutputFormat.setOutputPath(bprJob, new Path(args[1] + (++passCount)));
			bprJob.waitForCompletion(true);
			
			residualError = (double) bprJob.getCounters().findCounter(BPRCounters.RESIDUAL_ERROR).getValue();
			// Convert residual error back to double and divide by number of blocks to get average
			residualError = residualError / (NUM_BLOCKS*COUNTER_FACTOR);
			
			System.out.println("Residual error for iteration " + passCount 
					+ ": " + String.format("%.6f", residualError));

			// Reset residual error counter
			bprJob.getCounters().findCounter(BPRCounters.RESIDUAL_ERROR).setValue(0L);
			passCount++;
				
		} while(residualError > RESIDUAL_ERROR_THRESHOLD);
	}
}
