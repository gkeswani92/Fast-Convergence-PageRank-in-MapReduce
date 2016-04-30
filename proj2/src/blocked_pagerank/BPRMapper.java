package blocked_pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class BPRMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	Logger logger = Logger.getLogger(getClass());
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//logger.info("-------------------------------------");
		//logger.info("Received value: "+value.toString());
	
		String line = value.toString();
		line = line.trim();
		
		String [] filter = line.split("\\;");
		line = filter[filter.length-1];
		//logger.info("Filtered value: "+value.toString());
		
		// Get node, page rank and edges information from the file
		String [] parts = line.split(Constants.DELIMITER_REGEX);
		Integer nodeId = Integer.parseInt(parts[0]);
		Double pageRank = Double.parseDouble(parts[1]);
		String edges = parts.length == 3 ? parts[2]:"";
		Long blockId = blockIDofNode(nodeId);

		// For PAGERANK MESSAGE
		// Key: blockID of current node
		// Value: PR$nodeID$pageRank$outgoing_edges
		LongWritable outKey = new LongWritable(blockId);
		Text outValue = new Text(Constants.PAGE_RANK + Constants.DELIMITER + nodeId.toString() 
				+ Constants.DELIMITER + pageRank.toString() + Constants.DELIMITER + edges);
		context.write(outKey, outValue);
		
		// Do this only if we have outgoing edges from the current node
		if (!edges.isEmpty()) {
			String[] edgesArray = edges.split(",");
			
			for (String e: edgesArray) {
				
				// Find the block id of the destination node
				Long outgoingBlockId = new Long(blockIDofNode(Long.parseLong(e)));
				outKey = new LongWritable(outgoingBlockId);
				
				// If 2 nodes in same block, this is an internal page rank transfer
				// Value: BE$sourceID$destinationID
				if (outgoingBlockId.equals(blockId)) {
					outValue = new Text(Constants.EDGES_FROM_BLOCK + Constants.DELIMITER 
							+ nodeId.toString() + Constants.DELIMITER + e);
					//logger.info("Edge between 2 nodes in the same block");
				} 
				
				// If the source and the destination are in different blocks
				else {
					// Page rank factor which will go from the the source to the desination
					// Key: blockID of current node
					// Value: BC$sourceID$destinationID$PR_factor
					Double R = new Double(pageRank/edgesArray.length);
					outValue = new Text(Constants.BOUNDARY_CONDITION + Constants.DELIMITER 
							+ nodeId.toString() + Constants.DELIMITER
							+ e + Constants.DELIMITER + R.toString());
					//logger.info("Edge between 2 nodes in different blocks");
				}
				context.write(outKey, outValue);
			}
		}	
	}
	
	public long blockIDofNode(long nodeID) {
		
		int[] blocks = { 0, 10328, 20373, 30629, 40645,
				50462, 60841, 70591, 80118, 90497, 100501, 110567, 120945,
				130999, 140574, 150953, 161332, 171154, 181514, 191625, 202004,
				212383, 222762, 232593, 242878, 252938, 263149, 273210, 283473,
				293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929,
				374236, 384554, 394929, 404712, 414617, 424747, 434707, 444489,
				454285, 464398, 474196, 484050, 493968, 503752, 514131, 524510,
				534709, 545088, 555467, 565846, 576225, 586604, 596585, 606367,
				616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230 };

		int blockID = (int) Math.floor(nodeID / 10000);
		int boundary = blocks[blockID];
		if (nodeID < boundary) {
			blockID--;
		}
		return blockID;
	}
}
