package simple_pagerank;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class NodeResidualReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("IN REDUCER");
		long source_node = key.get();
		System.out.println("THE KEY IS: " + source_node);

		double sum = 0.0;
		double old_pr_val = 1 / 685229;
		double new_pr_val = 0.0;
		int num_outgoing_edges = 0;
		double cur_residual = 0.0;
		
		ArrayList<Long> outgoing_nodes = new ArrayList<Long>();
		Long cur_incoming_node;
		
		for (Text val : values) {
			StringTokenizer itr = new StringTokenizer(val.toString());
			String identifier = itr.nextToken();
			System.out.println("Identifier: " + identifier);
			
			if (identifier.equals("ME")) {
				old_pr_val = Double.parseDouble(itr.nextToken());
				System.out.println("OLD PR VAL: " + old_pr_val);
				
				num_outgoing_edges = Integer.parseInt(itr.nextToken());
				System.out.println("NUM OUTGOING EDGES: " + num_outgoing_edges);
				
				cur_incoming_node = Long.parseLong(itr.nextToken());
				outgoing_nodes.add(cur_incoming_node);
			} else {
				sum += Double.parseDouble(itr.nextToken());
			}
		}
		
		System.out.println("TOTAL I've RECV is: " + sum);
		new_pr_val = (0.15 / 685229 + 0.85 * sum);
		System.out.println("NEW PR VAL: " + new_pr_val);
		
		// Calculating the current residual
		cur_residual = (Math.abs(new_pr_val - old_pr_val) / new_pr_val);
		System.out.println("CUR RESIDUAL: " + cur_residual);

		context.getCounter(CounterEnums.LATEST_RESIDUAL).increment((long) (cur_residual * 100000000));

		for (long node : outgoing_nodes) {
			Text new_text = new Text(" " + String.valueOf(node) + " " + String.valueOf(new_pr_val) + " "
					+ String.valueOf(num_outgoing_edges));
			context.write(key, new_text);
		}

	}
}