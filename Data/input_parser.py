import os, re

global fromNetID
global rejectMin
global rejectLimit

# compute filter parameters for netid bi49
fromNetID = float(0.94)
rejectMin = float(float(0.9) * float(fromNetID))
rejectLimit = float(float(rejectMin) + float(0.01))

def selectInputLine(x):
	global fromNetID
	global rejectMin
	global rejectLimit
	if ((float(x) >= float(rejectMin)) and (float(x) < float(rejectLimit))):
		return "0"
	return "1"

def main():
	txt = open("94_edges.txt")
	new_file = open("trial.txt", "w+")
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

if __name__ == "__main__":
	main()