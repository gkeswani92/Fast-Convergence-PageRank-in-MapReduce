# Fast-Convergence-PageRank-in-MapReduce

### How to compute the set of files we are working with?
###### Source File: https://s3.amazonaws.com/cs5300s16-bi49-tmm259-gk368-p2/<xyz>.txt (replace <xyz>)
		1) Have edges.txt in the same directory as the python file
		2) Run $ python simple/blocked_parser.py
		3) Collect filtered vertices list from 94_edges.txt
		4) Get the output from Simple_PageRank.txt or Blocked_PageRank.txt

### Filtered Data Statistics
		0) netid:bi49 	rejectMin: 0.846 	rejectLimit: 0.856
		1) Total Edges in New Set: 7524549 (Total,7600595)
		2) Filtered In Percentage: 0.989994730676

### Input File Format
##### - Simple PageRank MapReduce Tasks
###### Source File: https://s3.amazonaws.com/cs5300s16-bi49-tmm259-gk368-p2/Simple_PageRank.txt
		- For the Simple PageRank computation, we have a file called Simple_PageRank.txt which is created from edges.txt via simple_parser.py. It outputs a new format of <source_node> <dest_node> <PageRank Value> <Number of Outgoing Edges> where delimeter is space ' '

##### - Blocked PageRank MapReduce Tasks
###### Source File: https://s3.amazonaws.com/cs5300s16-bi49-tmm259-gk368-p2/Blocked_PageRank.txt
		- For the Blocked PageRank computation, we have a file called Blocked_PageRank.txt which is created from edges.txt via blocked_parser.py. It outputs a new format of <src_node>$<PageRank Value>$[comma-seperated list of out going edges] where delimeter is '$'

### Simple PageRank
		Our Simple PageRank utilizes the counters so runs in 5 MapReduce tasks. Our Main, Mapper and Reducer Classes can be sumarized as below:
###### File Format: <src_node>      <dest_node>      <PR_val>    <number_of_outgoing_edges>		

###### Source File: Simple_PageRank.java
		1) Main: Sets our mapper and reducer classes as NodeMapper and NodeResidualReducer respectively. Sets output key, value as LongWritable and Text. Iterates for 5 map-reduce tasks, creates a folder output name with respect to the second argument provided (in e.g if its ../output it would create folders output0, ouput1 etc.). Our main runs for 5 loops only as we utilize the global counter, by collecting each nodes residual from their reducer and dividing it by the number of total nodes in the system, to find the average (along with a normalization constant of 10^8 to mantain precision of floating number, as counters are only types of Long). The class finally displays the average change in residual across 685230 nodes in the system.
###### Source File: NodeMapper.java
        2) Mapper: Has key value of LongWritable, Text and spits out LongWritable, Text for the reducer. Parses the input file for the source node, destination node, page-rank value and number of ougoing edges for each line of the file. For each of the destination nodes it sends the calculated PR value from the current source node to the destination node (by dividing current pagerank value by number of outgoing edges) with a special initial tag of "OTHERS". For each of the destination nodes it also sends metadata to itself to be able to generate the file for the next round. This message starts with the tag "ME" and has the metadata of the current PageRank Value, number of outgoing edges from this node and the destination node sending this information to the current source node. 
###### Source File: NodeResidualReducer.java
        2) Reducer: The reducer takes input key as a LongWritable refering to node id and values as list of Text. The reducer goes through the list of Text, and parses the text according to it's inital tag which was assigned as either "ME" or "OTHERS" by the mapper. If it's the "OTHERS" tag, it means the incoming message is the incoming pagerank value from another node for this round, so it sums up all these values to be able to caluclate it's new pagerank value. If it's the "ME" tag, it is information that is to be used to generate the next input file. So, it creates a list of all the nodes the current node sends pagerank values to, gets the previous pagerank value (for residual change calculations) and the number of out-going edges from this node. The new pagerank value is calculate according to the Surfer Model by summing up; 15 percent of the time we jump to a random node(0.15/685230), otherwise; 85 percent of the time its the PageRank value we collected(0.85*received_PR_value) from incoming edges. We calculate our residual by finding the difference between our old and new pagerank values and dividing this value by our new calculated pagerank value. We also utlize the global counters to be able to report from our main class the average change in residual across all nodes. This is done by sending the residual value, first by de-normalizing it (by multiplying it with a factor of 10^8) so that we can have the float/double precission we need. The last step generates the file for the last round.

### Blocked PageRank
	Our Blocked PageRank utilizes the counters and converges in 6 map reducer iterations. Our Main, Mapper and Reducer Classes can be summarized as below:

###### Source File: Block_PageRank.java
		1) Main: 
###### Source File: BPRMapper.java
        2) Mapper:
###### Source File: BPRReducer.java
        2) Reducer:		

### Results 
##### - Simple PageRank - Average Residual Errors in Each MapReduce Pass
		0) Average Residual Error's Updated Value is: 2.3270748
			- Node: 0 PageRank Value: 1.5114821464219157E-5 
			- Node: 1 PageRank Value: 5.267111562630828E-5 
		1) Average Residual Error's Updated Value is: 0.32670537
			- Node: 0 PageRank Value: 6.447241803516533E-6
			- Node: 1 PageRank Value: 4.979968738336481E-5
		2) Average Residual Error's Updated Value is: 0.19784908
			- Node: 0 PageRank Value: 2.9470947991453282E-5
			- Node: 1 PageRank Value: 6.455414771622583E-5 
		3) Average Residual Error's Updated Value is: 0.10070821
			- Node: 0 PageRank Value: 1.7665185465037863E-5
			- Node: 1 PageRank Value: 6.058341533818806E-5
		4) Average Residual Error's Updated Value is: 0.069692
			- Node: 0 PageRank Value: 1.6466880835062683E-5
			- Node: 1 PageRank Value: 6.0120305704489935E-5

##### - Jacobi Blocked PageRank - Average Residual Errors in Each MapReduce Pass	
		0) Average residual error after iteration 0: 2.806003712352941
		1) Average residual error after iteration 1: 0.038160
		2) Average residual error after iteration 2: 0.023614
		3) Average residual error after iteration 3: 0.009774
		4) Average residual error after iteration 4: 0.003799
		5) Average residual error after iteration 5: 0.000974

##### - Gauss Seidel Blocked PageRank - Average Residual Errors in Each MapReduce Pass	
		0) Average residual error after iteration 0: 2.807087
		1) Average residual error after iteration 1: 0.038735
		2) Average residual error after iteration 2: 0.024959
		3) Average residual error after iteration 3: 0.010955
		4) Average residual error after iteration 4: 0.004942
		5) Average residual error after iteration 5: 0.001870
		6) Average residual error after iteration 6: 0.000808
		
##### - Blocked PageRank With Random Partitioning - Average Residual Errors in Each MapReduce Pass	
		0) Average residual error after iteration 0: 2.338398
		1) Average residual error after iteration 1: 0.320930
		2) Average residual error after iteration 2: 0.190345
		3) Average residual error after iteration 3: 0.092883
		4) Average residual error after iteration 4: 0.061320
		5) Average residual error after iteration 5: 0.033192
		6) Average residual error after iteration 6: 0.026360
		7) Average residual error after iteration 7: 0.016122
		8) Average residual error after iteration 8: 0.013767
		9) Average residual error after iteration 9: 0.009408
		10) Average residual error after iteration 10: 0.008029 
		
