package gaussseidel_pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Constants;
import utils.Node;

public class GaussSeidelReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		//logger.info("-------------------------------------");
		//logger.info("Insider Reducer for block ID: "+key);
		Map<Long, Double> newPageRank = new HashMap<Long, Double>();
		Map<Long, Node> allNodes = new HashMap<Long,Node>();
		Map<String, List<String>> blockEdges = new HashMap<String, List<String>>();
		Map<String, Double> boundaryConditions = new HashMap<String, Double>();
		
		Map<String, Long> minNodes = new HashMap<String, Long>();
		minNodes.put("minimum", Long.MAX_VALUE);
		minNodes.put("second_minimum", Long.MAX_VALUE);
		
		for(Text t: values){
			String [] input = t.toString().split(Constants.DELIMITER_REGEX);
			
			// A message received by the reducer can be a PR, BE or BC
			if (Constants.PAGE_RANK.equals(input[0])) {
				
				// Create new node, set node id, page rank, edges and degree
				Node node = new Node();
				node.setNodeId(Long.parseLong(input[1]));
				node.setPageRank(Double.parseDouble(input[2]));
				newPageRank.put(node.getNodeId(), node.getPageRank());
				//logger.info("Received page rank from mapper:"+Double.parseDouble(input[2]));
				
				if (input.length == 4) {
					node.setEdges(input[3]);
					node.setDegree(input[3].split(",").length);
				}
				
				// Maintaining a list of all nodes in the current block
				allNodes.put(node.getNodeId(), node);
				
				// Keeping tracking of the two minimum nodes in this block
				if(node.getNodeId().compareTo(minNodes.get("minimum")) < 0){
					minNodes.put("second_minimum", minNodes.get("minimum"));
					minNodes.put("minimum", node.getNodeId());
				} else if(node.getNodeId().compareTo(minNodes.get("second_minimum")) < 0){
					minNodes.put("second_minimum", node.getNodeId());
				} 
				
			} else if (Constants.EDGES_FROM_BLOCK.equals(input[0])) {
				//logger.info("Inside edges from block");
				
				// Creates a list of edges coming into each node
				if (!blockEdges.containsKey(input[2])) {
					List<String> edges = new ArrayList<String>();
					edges.add(input[1]);
					blockEdges.put(input[2], edges);
				} else {
					blockEdges.get(input[2]).add(input[1]);
				}
			} else if (Constants.BOUNDARY_CONDITION.equals(input[0])) {
				//logger.info("Inside boundary condition");
				
				// Maintains a mapping of the collective page rank coming in to a node
				// from all other nodes of different blocks
				if (!boundaryConditions.containsKey(input[2])) {
					boundaryConditions.put(input[2], 0.0);
				}
				boundaryConditions.put(input[2], 
						boundaryConditions.get(input[2]) + Double.parseDouble(input[3]));
			}
		}
	}
}
