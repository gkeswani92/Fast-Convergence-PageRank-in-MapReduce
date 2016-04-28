package simple_pagerank;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("IN MAPPER");

		// Getting details on the source, destionation, current page rank and
		// number of outgoing edges
		StringTokenizer itr = new StringTokenizer(value.toString());
		long source_node = Long.parseLong(itr.nextToken());
		long dest_node = Long.parseLong(itr.nextToken());
		double cur_pr = Double.parseDouble(itr.nextToken());
		int num_outgoing_edges = Integer.parseInt(itr.nextToken());

		System.out.println("S_NODE: " + source_node);
		System.out.println("D_NODE: " + dest_node);
		System.out.println("CUR_PR: " + cur_pr);
		System.out.println("NUM_EDGES: " + num_outgoing_edges);

		Text send_source = new Text("ME " + String.valueOf(cur_pr) + " " + String.valueOf(num_outgoing_edges) + " "
				+ String.valueOf(dest_node));
		System.out.println("ME " + String.valueOf(cur_pr) + " " + String.valueOf(num_outgoing_edges) + " "
				+ String.valueOf(dest_node));

		context.write(new LongWritable(source_node), send_source);

		// Send current source's page rank contribution to its neighbors by
		// equally diving the page rank between all outgoing edges
		Text send_dest = new Text(
				"OTHERS " + String.valueOf(cur_pr / num_outgoing_edges) + " " + String.valueOf(source_node));
		System.out.println("OTHERS " + String.valueOf(cur_pr / num_outgoing_edges) + " " + String.valueOf(source_node));
		context.write(new LongWritable(dest_node), send_dest);

	}
}