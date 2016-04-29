package simple_pagerank;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class NodeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());
		//get source_node
		long source_node = Long.parseLong(itr.nextToken());
		//get destination node
		long dest_node = Long.parseLong(itr.nextToken());
		//get source node's current PR value
		double cur_pr = Double.parseDouble(itr.nextToken());
		//get the number of outgoing edges from source node
		int num_outgoing_edges = Integer.parseInt(itr.nextToken());
		//create message to send to source node, ME PR_VALUE NUM_OUTGOING_EDGES DESTINATION_NODE_SENDING_THE_METADATA
		Text send_source = new Text("ME " + String.valueOf(cur_pr) + " " + String.valueOf(num_outgoing_edges) + " " + String.valueOf(dest_node));
		//send message to source_node
		context.write(new LongWritable(source_node), send_source);
		//create message to send to destination node, OTHERS PR_VAL
		Text send_dest = new Text("OTHERS " + String.valueOf(cur_pr/num_outgoing_edges));
		//send message to destination node
		context.write(new LongWritable(dest_node), send_dest);
		
    }
}