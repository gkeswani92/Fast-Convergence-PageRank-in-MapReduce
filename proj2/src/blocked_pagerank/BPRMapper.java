package blocked_pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BPRMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.trim();
		String [] parts = line.split(BlockPageRank.DELIMITER);
		
		Integer nodeId = Integer.parseInt(parts[0]);
		Double pageRank = Double.parseDouble(parts[1]);
		Long blockId = blockIDofNode(nodeId);
		String edges = "";
		
		if ( parts.length == 4) {
			edges = parts[2];
		}
		
		// Key: blockID
		// Value: rank$nodeID$pageRank$outgoing_edges
		Text outKey = new Text(blockId.toString());
		Text outValue = new Text(BlockPageRank.PAGE_RANK + BlockPageRank.DELIMITER + nodeId.toString() 
				+ BlockPageRank.DELIMITER + pageRank.toString() + BlockPageRank.DELIMITER + edges);
		
		context.write(outKey, outValue);
		
		if (!edges.isEmpty()) {
			String[] edgesArray = edges.split(",");
			
			for (String e: edgesArray) {
				Long outgoingBlockId = new Long(blockIDofNode(Long.parseLong(e)));
				outKey = new Text(outgoingBlockId.toString());
				
				// If 2 nodes in same block
				if (outgoingBlockId == blockId) {
					outValue = new Text(BlockPageRank.EDGES_FROM_BLOCK + BlockPageRank.DELIMITER 
							+ nodeId.toString() + BlockPageRank.DELIMITER + e);
				} 
				
				// Edge block
				else {
					// Page rank factor
					Double R = new Double(pageRank/edgesArray.length);
					outValue = new Text(BlockPageRank.BOUNDARY_CONDITION + BlockPageRank.DELIMITER 
							+ nodeId.toString() + BlockPageRank.DELIMITER
							+ e + BlockPageRank.DELIMITER + R.toString());
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
		
		int blockId = (int) (nodeID/10000);
		if(blocks[blockId] < nodeID) {
			return blockId + 1;
		} else if (blockId == 0 || blocks[blockId-1] < nodeID) {
			return blockId;
		} else {
			return blockId - 1;
		}
	}
}
