package simple_pagerank;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class NodeResidualReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	//create metadata for the function
    	double sum = 0.0;
    	double old_pr_val = 1/685229;
    	double new_pr_val = 0.0;
    	int num_outgoing_edges = 0;
    	double cur_residual = 0.0;
    	ArrayList<Long> outgoing_nodes = new ArrayList<Long>();
    	Long cur_incoming_node;
    	//loop through all the values received
      	for (Text val : values) {
      		//the delimeter is " ", so we tokenize
      		StringTokenizer itr = new StringTokenizer(val.toString());
      		String identifier = itr.nextToken();
      		//info sent from myself to generate info for next round
      		if (identifier.equals("ME")){
      			//get old PR value from last round
      			old_pr_val = Double.parseDouble(itr.nextToken());
      			//get number of outgoing edges from this node
      			num_outgoing_edges = Integer.parseInt(itr.nextToken());
      			//get the nodes that this node sends PR values to
      			cur_incoming_node = Long.parseLong(itr.nextToken());
      			//add it to our outgoing_nodes list to generate for next round
      			outgoing_nodes.add(cur_incoming_node);	
      		}
      		//info sent from others, feeding me PR values
      		else{
      			//sum them all up
      			sum += Double.parseDouble(itr.nextToken());
      		}
      	}
      	//update pr value based on surfers model
      	new_pr_val = (0.15/685230 + 0.85*sum);
      	//calculate the change in PR values to get Residual value
      	cur_residual = (Math.abs(new_pr_val-old_pr_val)/new_pr_val);
      	//Increment the global counter after de-normalizing by 10^8 to keep the significant figures
      	context.getCounter(CounterEnums.LATEST_RESIDUAL).increment((long)(cur_residual*100000000));
      	//generate file for next round
      	for(long node: outgoing_nodes){
      		Text new_text = new Text(" " + String.valueOf(node) + " " + String.valueOf(new_pr_val) + " " + String.valueOf(num_outgoing_edges));
      		context.write(key, new_text);
		}
      	
    }
}