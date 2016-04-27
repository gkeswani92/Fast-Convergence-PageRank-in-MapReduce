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
	global start_PR
	txt = open("test.txt")
	cur_source_node = str(0)
	count = 0
	my_map = dict()
	line_split = list()
	for line in txt.readlines():
		line_split = line.split()
		print line
		if(str(line_split[0]) == cur_source_node):
			count += 1
		else:
			my_map[cur_source_node] = str(count)
			count = 1
			cur_source_node = str(line_split[0])
	#for the last one
	my_map[str(line_split[0])] = str(count)
	print my_map
	txt.close()
	txt = open("test.txt")
	new_file = open("Processed.txt", "w+")
	for line in txt.readlines():
		line_split = line.split()
		new_line = line_split[0] + " " + line_split[1] + " " + str(start_PR) + " " + my_map[str(line_split[0])] + "\n"
		new_file.write(new_line)
	new_file.close()
	txt.close()

def main():
	"""
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
	"""
	additional_process()
if __name__ == "__main__":
	main()

#Node1 Node2 PR_VAL NUM_OUTGOING