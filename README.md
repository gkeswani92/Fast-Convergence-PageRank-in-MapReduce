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
		# For the Simple PageRank computation, we have a file called Simple_PageRank.txt which is created from edges.txt via simple_parser.py. It outputs a new format of <source_node> <dest_node> <PageRank Value> <Number of Outgoing Edges> where delimeter is space ' '

##### - Blocked PageRank MapReduce Tasks
###### Source File: https://s3.amazonaws.com/cs5300s16-bi49-tmm259-gk368-p2/Blocked_PageRank.txt
		- For the Blocked PageRank computation, we have a file called Blocked_PageRank.txt which is created from edges.txt via blocked_parser.py. It outputs a new format of <src_node>$<PageRank Value>$[comma-seperated list of out going edges] where delimeter is '$'

### Simple PageRank
		Our Simple PageRank utilizes the counters so runs in 5 MapReduce tasks. Our Main, Mapper and Reducer Classes can be sumarized as below:
###### Source File: Simple_PageRank.java
		1) Main: 
###### Source File: NodeMapper.java
        2) Mapper:
###### Source File: NodeResidualReducer.java
        2) Reducer:

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
		1) Average Residual Error's Updated Value is: 0.32670537
		2) Average Residual Error's Updated Value is: 0.19784908
		3) Average Residual Error's Updated Value is: 0.10070821
		4) Average Residual Error's Updated Value is: 0.069692

##### - Blocked PageRank - Average Residual Errors in Each MapReduce Pass	
		0) Average residual error after iteration 0: 2.806003712352941
		1) Average residual error after iteration 1: 0.038160
		2) Average residual error after iteration 2: 0.023614
		3) Average residual error after iteration 3: 0.009774
		4) Average residual error after iteration 4: 0.003799
		5) Average residual error after iteration 5: 0.000974

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
		
