package blocked_pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import blocked_pagerank.BPRCounter;

public class BPRReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	private static final int MAX_ITERATIONS = 5;
	private static final Double DAMPING_FACTOR = 0.85;
	private static final int NUM_NODES = 685230;
	
	Logger logger = Logger.getLogger(getClass());

	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Map<Long, Double> newPageRank = new HashMap<Long, Double>();
		List<BPRNode> allNodes = new ArrayList<BPRNode>();
		Map<String, List<String>> blockEdges = new HashMap<String, List<String>>();
		Map<String, Double> boundaryConditions = new HashMap<String, Double>();
		
		for(Text t: values){
			String [] input = t.toString().split(BlockPageRank.DELIMITER_REGEX);
			
			if (BlockPageRank.PAGE_RANK.equals(input[0])) {
				BPRNode node = new BPRNode();
				node.setNodeId(Long.parseLong(input[1]));
				node.setPageRank(Double.parseDouble(input[2]));
				newPageRank.put(node.getNodeId(), node.getPageRank());
				
				if (input.length == 4) {
					node.setEdges(input[3]);
					node.setDegree(input[3].split(",").length);
				}
				
				allNodes.add(node);
			} else if (BlockPageRank.EDGES_FROM_BLOCK.equals(input[0])) {
				
				logger.info("Inside edges from block");
				if (!blockEdges.containsKey(input[2])) {
					List<String> edges = new ArrayList<String>();
					edges.add(input[1]);
					blockEdges.put(input[2], edges);
				} else {
					blockEdges.get(input[2]).add(input[1]);
				}
			} else if (BlockPageRank.BOUNDARY_CONDITION.equals(input[0])) {
				
				logger.info("Inside boundary condition");
				if (!boundaryConditions.containsKey(input[0])) {
					boundaryConditions.put(input[2], 0.0);
				}
				
				boundaryConditions.put(input[2], 
						boundaryConditions.get(input[2]) + Double.parseDouble(input[3]));
			}
		}
		
		if(!allNodes.isEmpty()) {
			logger.info("Calculating page rank");
			int numIterations = 0;
			Double residualError = 0.0;
			while (numIterations < MAX_ITERATIONS || residualError < BlockPageRank.RESIDUAL_ERROR_THRESHOLD) {
				residualError = iterateBlockOnce(allNodes, newPageRank, blockEdges, boundaryConditions);
				numIterations++;
			}
			logger.info("In block residual error is " + residualError);

			residualError = 0.0;
			for (BPRNode node : allNodes) {
				logger.info("Old Page Rank: " + node.getPageRank());
				logger.info("New Page Rank: " + newPageRank.get(node.getNodeId()));
				residualError += Math.abs((newPageRank.get(node.getNodeId()) - node.getPageRank())) / newPageRank.get(node.getNodeId());
			}
			logger.info("All node residual error is " + residualError);

			residualError = residualError / allNodes.size();
			logger.info("Average residual error is " + residualError);

			for (BPRNode node : allNodes) {
				String outValue = newPageRank.get(node.getNodeId()) + BlockPageRank.DELIMITER + node.getDegree()
						+ BlockPageRank.DELIMITER + node.getEdges();

				// Text outKey = new Text(node.getNodeId().toString());
				LongWritable outKey = new LongWritable(node.getNodeId());
				context.write(outKey, new Text(outValue));
			}

			// Convert residual value to long to store into counter
			Long residualValue = (long) (residualError * BlockPageRank.COUNTER_FACTOR);
			logger.info("Value being written to counter is " + residualValue);
			context.getCounter(BPRCounter.RESIDUAL_ERROR).increment(residualValue);
			cleanup(context);
		}
	}
	
	private Double iterateBlockOnce(List<BPRNode> allNodes, Map<Long, Double> newPageRank, Map<String, List<String>> blockEdges, Map<String, Double> boundaryConditions) {
		
		Double residualError = 0.0;
		Map<Long, Double> PRMap = new HashMap<Long, Double>();

		for(BPRNode node: allNodes) {
			Double oldPR = newPageRank.get(node.getNodeId());
			Double newPR = 0.0;
			
			if(blockEdges.containsKey(node.getNodeId())) {
				
				for(String edge: blockEdges.get(node.getNodeId())) {
					BPRNode n = getNodeFromId(allNodes, edge);
					newPR += newPageRank.get(edge)/n.getDegree();
				}
			}
			
			if (boundaryConditions.containsKey(node.getNodeId())) {
				newPR += boundaryConditions.get(node.getNodeId());
			}
			
			newPR = (newPR * DAMPING_FACTOR) + (0.15/NUM_NODES);
			PRMap.put(node.getNodeId(), newPR);
			residualError += Math.abs(newPR - oldPR) / newPR;
			
			logger.info("Old Page Rank: " + oldPR + " New Page Rank: " + newPR);
		}
		
		newPageRank.clear();
		newPageRank.putAll(PRMap);
		residualError = residualError/allNodes.size();
		return residualError;
	}
	
	private BPRNode getNodeFromId(List<BPRNode> allNodes, String node) {
		
		for(BPRNode n: allNodes) {
			if(n.getNodeId().equals(Long.parseLong(node))) {
				return n;
			}
		}
		
		return null;
	}
}
