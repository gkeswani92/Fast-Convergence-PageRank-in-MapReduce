import os, re

global fromNetID
global rejectMin
global rejectLimit
global start_PR
# compute filter parameters for netid bi49
fromNetID = float(0.94)
rejectMin = float(float(0.9) * float(fromNetID))
rejectLimit = float(float(rejectMin) + float(0.01))
start_PR = float(1)/float(685229)

def selectInputLine(x):
	global fromNetID
	global rejectMin
	global rejectLimit
	if ((float(x) >= float(rejectMin)) and (float(x) < float(rejectLimit))):
		return "0"
	return "1"

def additional_process():
	#sourceNodeID$pageRank$degree$edges
	global start_PR
	txt = open("94_edges.txt")
	cur_source_node = str(0)
	outgoing_edges = list()
	my_map = dict()
	line_split = list()
	for line in txt.readlines():
		line_split = line.split()
		#print line
		if(str(line_split[0]) == cur_source_node):
			outgoing_edges.append(line_split[1])
		else:
			#if (len(outgoing_edges) == 1)
			my_map[cur_source_node] = outgoing_edges
			cur_source_node = str(line_split[0])
			outgoing_edges = list()
			outgoing_edges.append(line_split[1])

	#for the last one
	my_map[str(line_split[0])] = outgoing_edges
	#print my_map
	txt.close()
	#print "Finished Creating My Map"
	txt = open("94_edges.txt")
	new_file = open("Blocked_PageRank.txt", "w+")
	map_processed = dict()
	count = 0
	steps = 1
	for line in txt.readlines():
		line_split = line.split()
		if (line_split[0] in map_processed): continue
		new_line = line_split[0] + "$" + str(start_PR) + "$"
		for elt in my_map[str(line_split[0])]:
			new_line += (elt + ",")
		new_line = new_line[0:len(new_line)-1] + "\n"
		new_file.write(new_line)
		map_processed[line_split[0]] = "1"
		count += 1
		if (count % 5000 == 0): 
			print "Done with " + str(5000*steps) + " Lines"
			steps += 1
	print "DONE WITH ALL LINES"
	new_file.close()
	txt.close()

def main():
	
	txt = open("edges.txt")
	new_file = open("94_edges.txt", "w+")
	count = 0
	added = 0
	for line in txt.readlines():
		line_split = line.split()
		new_line = str()
		if(selectInputLine(line_split[2]) == "1"):
			new_line = line_split[0] + " " + line_split[1] + "\n"
			new_file.write(new_line)
			new_line = str()
			added += 1
		count += 1
	txt.close()
	new_file.close()
	print "Total Vertices Seen: " + str(count)
	print "Total in New Set: " + str(added)
	print "Filtered In Percentage: " + str(float(added)/float(count))
	
	additional_process()
if __name__ == "__main__":
	main()

#Node1 Node2 PR_VAL NUM_OUTGOING