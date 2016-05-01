package gaussseidel_pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import utils.Constants;
import utils.CustomCounter;
import utils.Node;

public class GaussSeidelReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	Logger logger = Logger.getLogger(getClass());

	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// logger.info("-------------------------------------");
		// logger.info("Insider Reducer for block ID: "+key);
		Map<Long, Double> newPageRank = new HashMap<Long, Double>();
		Map<Long, Double> backUpPageRank = new HashMap<Long, Double>();
		Map<Long, Node> allNodes = new TreeMap<Long, Node>();
		Map<String, List<String>> blockEdges = new HashMap<String, List<String>>();
		Map<String, Double> boundaryConditions = new HashMap<String, Double>();

		Map<String, Long> minNodes = new HashMap<String, Long>();
		minNodes.put("minimum", Long.MAX_VALUE);
		minNodes.put("second_minimum", Long.MAX_VALUE);

		for (Text t : values) {
			String[] input = t.toString().split(Constants.DELIMITER_REGEX);

			// A message received by the reducer can be a PR, BE or BC
			if (Constants.PAGE_RANK.equals(input[0])) {

				// Create new node, set node id, page rank, edges and degree
				Node node = new Node();
				node.setNodeId(Long.parseLong(input[1]));
				node.setPageRank(Double.parseDouble(input[2]));
				newPageRank.put(node.getNodeId(), node.getPageRank());
				backUpPageRank.put(node.getNodeId(), node.getPageRank());
				// logger.info("Received page rank from
				// mapper:"+Double.parseDouble(input[2]));

				if (input.length == 4) {
					node.setEdges(input[3]);
					node.setDegree(input[3].split(",").length);
				}

				// Maintaining a list of all nodes in the current block
				allNodes.put(node.getNodeId(), node);

				// Keeping tracking of the two minimum nodes in this block
				if (node.getNodeId().compareTo(minNodes.get("minimum")) < 0) {
					minNodes.put("second_minimum", minNodes.get("minimum"));
					minNodes.put("minimum", node.getNodeId());
				} else if (node.getNodeId().compareTo(minNodes.get("second_minimum")) < 0) {
					minNodes.put("second_minimum", node.getNodeId());
				}

			} else if (Constants.EDGES_FROM_BLOCK.equals(input[0])) {
				// logger.info("Inside edges from block");

				// Creates a list of edges coming into each node
				if (!blockEdges.containsKey(input[2])) {
					List<String> edges = new ArrayList<String>();
					edges.add(input[1]);
					blockEdges.put(input[2], edges);
				} else {
					blockEdges.get(input[2]).add(input[1]);
				}
			} else if (Constants.BOUNDARY_CONDITION.equals(input[0])) {
				// logger.info("Inside boundary condition");

				// Maintains a mapping of the collective page rank coming in to
				// a node
				// from all other nodes of different blocks
				if (!boundaryConditions.containsKey(input[2])) {
					boundaryConditions.put(input[2], 0.0);
				}
				boundaryConditions.put(input[2], boundaryConditions.get(input[2]) + Double.parseDouble(input[3]));
			}
		}

		if (!allNodes.isEmpty()) {
			int numIterations = 0;
			Double residualError = Double.MAX_VALUE;
			
			// Iterate over block until error < threshold or numIterations >
			// custom limit
			while (numIterations < Constants.MAX_ITERATIONS && residualError > Constants.RESIDUAL_ERROR_THRESHOLD) {
				residualError = iterateBlockOnce(allNodes, newPageRank, blockEdges, boundaryConditions, backUpPageRank);
				numIterations++;
			}
			// logger.info("In block residual error is " + residualError);

			residualError = 0.0;
			for (Node node : allNodes.values()) {
				residualError += Math.abs((newPageRank.get(node.getNodeId()) - node.getPageRank()))
						/ newPageRank.get(node.getNodeId());
			}
			// logger.info("All node residual error is " + residualError);

			residualError = residualError / allNodes.size();
			// logger.info("Average residual error is " + residualError);

			for (Node node : allNodes.values()) {
				// logger.info("Emitting from reducer node ID: " + node.getNodeId() + " Page Rank: "+newPageRank.get(node.getNodeId()));
				String outValue = node.getNodeId() + Constants.DELIMITER + newPageRank.get(node.getNodeId())
						+ Constants.DELIMITER + node.getEdges();
				// logger.info("Emitting from reducer: " + outValue);

				LongWritable outKey = new LongWritable(node.getNodeId());
				context.write(outKey, new Text(outValue));
			}

			// Convert residual value to long to store into counter
			Long residualValue = (long) (residualError * Constants.COUNTER_FACTOR);
			//logger.info("Value being written to counter is " + residualValue);
			context.getCounter(CustomCounter.RESIDUAL_ERROR).increment(residualValue);
			
			CustomCounter cc = getCustomCounter(Long.parseLong(key.toString()));
			if (cc != null) {
				context.getCounter(cc).increment(numIterations);
			} else {
				context.getCounter(cc).increment(0);
			}
			cleanup(context);

			// logger.info("");
			// logger.info("Number of iterations in block " + key + " to coverge: " + numIterations);
			// logger.info("Page Rank of Node 0 i.e. Node ID : " +
			// allNodes.get(minNodes.get("minimum")).getNodeId().toString() + "
			// in block "+ key + " is :" +
			// allNodes.get(minNodes.get("minimum")).getPageRank().toString());
			// logger.info("Page Rank of Node 1 i.e. Node ID : " +
			// allNodes.get(minNodes.get("second_minimum")).getNodeId().toString()
			// + " in block "+ key + " is :" +
			// allNodes.get(minNodes.get("second_minimum")).getPageRank().toString());
		}
	}

	private Double iterateBlockOnce(Map<Long, Node> allNodes, Map<Long, Double> newPageRank,
			Map<String, List<String>> blockEdges, Map<String, Double> boundaryConditions,
			Map<Long, Double> backUpPageRank) {

		Double residualError = 0.0;

		for (Node node : allNodes.values()) {
			//logger.info("Current node id: "+node.getNodeId());

			Double newPR = 0.0;

			// Add page rank to current node if it has incoming edges
			// from other nodes in the same block
			if (blockEdges.containsKey(node.getNodeId().toString())) {
				// logger.info(node.getNodeId() + " has incoming edges");

				for (String edge : blockEdges.get(node.getNodeId().toString())) {
					//logger.info(node.getNodeId() + " has an incoming edge from "+edge);

					// Get the source node of the incoming edge and calculate its page rank contribution to mine
					Node n = getNodeFromId(allNodes, edge);
					//logger.info("Using page rank value of incoming edge: " + newPageRank.get(Long.parseLong(edge)) 
					//				+ "for calculating new PR of " + node.getNodeId());
					newPR += newPageRank.get(Long.parseLong(edge)) / n.getDegree();
				}
			}

			// Add page rank to current node if it has incoming edges
			// from other nodes in the other block
			if (boundaryConditions.containsKey(node.getNodeId().toString())) {
				//logger.info(node.getNodeId() + " has an incoming edge from outside the block with PR: " 
				//				+ boundaryConditions.get(node.getNodeId().toString()));
				newPR += boundaryConditions.get(node.getNodeId().toString());
			}

			// logger.info("New Page Rank before damping: " + newPR);
			newPR = (newPR * Constants.DAMPING_FACTOR) + (0.15 / Constants.NUM_NODES);
			// logger.info("New Page Rank after damping: " + newPR);
			newPageRank.put(node.getNodeId(), newPR);
			residualError += Math.abs(newPR - backUpPageRank.get(node.getNodeId())) / newPR;
			//logger.info("Node ID:" + node.getNodeId().toString() + " New Page Rank: " + newPR + "Residual Error: "+residualError);	
		}

		backUpPageRank.clear();
		backUpPageRank.putAll(newPageRank);
		residualError = residualError / allNodes.size();
		return residualError;
	}

	private Node getNodeFromId(Map<Long, Node> allNodes, String node) {
		if (allNodes.containsKey(Long.parseLong(node))) {
			return allNodes.get(Long.parseLong(node));
		}
		return null;
	}
	
	public CustomCounter getCustomCounter (Long blockId) {
		if (blockId.equals(0)) {
			return CustomCounter.ITERATIONS_BLOCK_1;
		} else if (blockId.equals(1)){
			return CustomCounter.ITERATIONS_BLOCK_2;
		} else if (blockId.equals(2)){
			return CustomCounter.ITERATIONS_BLOCK_3;
		} else if (blockId.equals(3)){
			return CustomCounter.ITERATIONS_BLOCK_4;
		} else if (blockId.equals(4)){
			return CustomCounter.ITERATIONS_BLOCK_5;
		} else if (blockId.equals(5)){
			return CustomCounter.ITERATIONS_BLOCK_6;
		} else if (blockId.equals(6)){
			return CustomCounter.ITERATIONS_BLOCK_7;
		} else if (blockId.equals(7)){
			return CustomCounter.ITERATIONS_BLOCK_8;
		} else if (blockId.equals(8)){
			return CustomCounter.ITERATIONS_BLOCK_9;
		} else if (blockId.equals(9)){
			return CustomCounter.ITERATIONS_BLOCK_10;
		} else {
			return null;
		}
	}
}
