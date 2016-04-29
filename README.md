# Fast-Convergence-PageRank-in-MapReduce

### How to compute the set of files we are working with?
		1) Have edges.txt in the same directory as the python file
		2) Run $ python simple/blocked_parser.py
		3) Collect filtered vertices list from 94_edges.txt
		4) Get the output from Simple_PageRank.txt or Blocked_PageRank.txt

	Our current files are located in https://s3.amazonaws.com/cs5300s16-bi49-tmm259-gk368-p2/<xyz>.txt and is made public

### Filtered Data Statistics
		0) netid:bi49 	rejectMin: 0.846 	rejectLimit: 0.856
		1) Total Edges in New Set: 7524549 (Total,7600595)
		2) Filtered In Percentage: 0.989994730676

### Input File Format for Simple and Blocked PageRank MapReduce Tasks
###### Source File: https://s3.amazonaws.com/cs5300s16-bi49-tmm259-gk368-p2/Simple_PageRank.txt
		# For the Simple PageRank computation, we have a file called Simple_PageRank.txt which is created from edges.txt via simple_parser.py. It outputs a new format of <source_node> <dest_node> <PageRank Value> <Number of Outgoing Edges> where delimeter is space ' '
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
### Results 
##### Simple PageRank - Average Residual Errors in Each MapReduce Pass
		0) Average Residual Error's Updated Value is: 2.3270748
		1) Average Residual Error's Updated Value is: 0.32670537
		2) Average Residual Error's Updated Value is: 0.19784908
		3) Average Residual Error's Updated Value is: 0.10070821
		4) Average Residual Error's Updated Value is: 0.069692
