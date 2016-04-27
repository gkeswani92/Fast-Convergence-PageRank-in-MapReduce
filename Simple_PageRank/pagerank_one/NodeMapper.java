package pagerank_one;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;

public class NodeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		System.out.println("IN MAPPER");
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		long source_node = Long.parseLong(itr.nextToken());
		System.out.println("S_NODE: " + source_node);
		
		long dest_node = Long.parseLong(itr.nextToken());
		System.out.println("D_NODE: " + dest_node);
		
		double cur_pr = Double.parseDouble(itr.nextToken());
		System.out.println("CUR_PR: " + cur_pr);
		
		int num_outgoing_edges = Integer.parseInt(itr.nextToken());
		System.out.println("NUM_EDGES: " + num_outgoing_edges);
		
		Text send_source = new Text("ME " + String.valueOf(cur_pr) + " " + String.valueOf(num_outgoing_edges) + " " + String.valueOf(dest_node));
		System.out.println("ME " + String.valueOf(cur_pr) + " " + String.valueOf(num_outgoing_edges) + " " + String.valueOf(dest_node));
		
		context.write(new LongWritable(source_node), send_source);
		Text send_dest = new Text("OTHERS " + String.valueOf(cur_pr/num_outgoing_edges) + " " + String.valueOf(source_node));
		System.out.println("OTHERS " + String.valueOf(cur_pr/num_outgoing_edges) + " " + String.valueOf(source_node));
		context.write(new LongWritable(dest_node), send_dest);
		
    }
}