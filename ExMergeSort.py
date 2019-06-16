import os
import math

#globals declared for incrementing
#use ctrl-f and search for 'read' and 'write' to pinpoint the locations where I increment pageread and pagewrite
pageRead = 0
pageWrite = 0
passes = 0

#function that sorts a bytearray given a field
#returns the sorted bytearray after evaluating it in utf-8 format
def sortPage(array, field):
	byteList = []
	for i in range(0, len(array), 64):
		if (array[i] == 0):
			break
		byteList.append(array[i:i+64])
	if (field == 1):
		byteList.sort(key=takeLastName)
	elif (field == 0):
		byteList.sort(key=takeFirstName)
	elif (field == 2):
		byteList.sort(key=takeEmail)
	sortedByteArray = bytearray()
	for elem in byteList:
		sortedByteArray += elem
	return sortedByteArray

#Helper functions below that takes a bytearray of length 64 and returns the string value of whatever key you look for
def takeLastName(elem):
	return elem[12:26].decode('utf-8')

def takeFirstName(elem):
	return elem[0:12].decode('utf-8')

def takeEmail(elem):
	return elem[26:64].decode('utf-8')

def getKey(field, element):
	if (field == 0):
		return takeFirstName(element)
	elif (field == 1):
		return takeLastName(element)
	elif (field == 2):
		return takeEmail(element)

#This function represents a merge pass(pass 1 and onwards)
#The merge function uses bPages in total to merge
#Length of meta is at most bPages-1 in length
def merge(inp, temp, meta, pSize, fSize, field):
#	print(meta)
	global pageRead
	global pageWrite
	if (len(meta) == 1):
		chunkSize = fSize
	else:
		chunkSize = meta[1] - meta[0]
#	print(chunkSize)
	#This is a list of bytearrays, each representing a page to be loaded into memory
	kList = []
	#This is a list of current index for each element in kList, represents where in the page we are at
	iList = []
	#This is a list of ints representing pages read for each match in kList, keep track of how many pages left for current chunk
	pList = []
	#Copies the meta list for modification, meta list is a list of starting index for each chunk from file
	thisMeta = meta.copy()
	readFrom = inp
	#For each chunk to be read, create an empty bytearray of 1 page size, so uses bPages-1 pages for storing input
	#For each chunk also initialize index of each page and number of pages used to 0
	for elem in meta:
		kList.append(bytearray(pSize))
		iList.append(0)
		pList.append(0)
	#go through kList and read 1 page of the chunk from the index specified by meta for each element in kList
	for num in range(len(kList)):
		readFrom.seek(meta[num])
		readFrom.readinto(kList[num])
		pageRead += 1
	#Creates an output buffer of 1 page size, so total of bPage pages are used for this function
	outBuffer = bytearray(pSize)
	#Current index on output buffer is 0
	outIndex = 0
	#While there are still chunks left to be processed, keep going
	while (len(kList) != 0):
		#For loop sets min value and index of min value
		min = None
		minIndex = None
		for i in range(len(kList)):
			cur = getKey(1, kList[i][iList[i]:iList[i]+64])
			if (min == None):
				min = cur
				minIndex = i
			else:
				if (cur < min):
					min = cur
					minIndex = i
		#Writes the record to the output buffer and increment out buffer index
		outBuffer[outIndex:outIndex+64] = kList[minIndex][iList[minIndex]:iList[minIndex]+64]
		outIndex += 64
		#If outbuffer is full then increment pageWrite and write the result to the outfile
		if (outIndex == pSize):
			temp.write(outBuffer)
			pageWrite += 1
			outIndex = 0
		#Increments input buffer of that chunk by 64 bytes as well
		iList[minIndex] += 64
		#If the entire buffer(a whole page) is read then check conditions
		if (iList[minIndex] == pSize):
			pList[minIndex] += 1
			#If reach EOF or end of chunk, then delete that chunk from all lists since no more data is coming in
#			print(pSize*pList[minIndex])
#			print(minIndex)
			if ((thisMeta[minIndex] + pList[minIndex]*pSize == fSize) or (pSize*pList[minIndex] == chunkSize)):
				del thisMeta[minIndex]
				del kList[minIndex]
				del iList[minIndex]
				del pList[minIndex]
			#Update conditions and read into the input buffer if everything's fine
			else:
				iList[minIndex] = 0
				#This isn't a new page, we're just resetting the page to readinto again
				kList[minIndex] = bytearray(pSize)
				readFrom.seek(thisMeta[minIndex] + pList[minIndex]*pSize)
				readFrom.readinto(kList[minIndex])
				pageRead += 1

#Main function that is called upon to sort a DB
def sortDB(input, output, bPage, pSize, field):
	global pageRead
	global pageWrite
	global passes
	file = open(input, "rb")
	out = open(output, "wb")
	#Meta is the list of starting index for each chunk in the file
	meta = []
	fSize = os.path.getsize(input)
	memSize = bPage*pSize
	#First runsize determines how many for loops are need for pass 0, the run generating pass
	runs = math.ceil(fSize/memSize)
	for num in range(runs):
		mem = bytearray(memSize)
		file.readinto(mem)
		mem = sortPage(mem, field)
		pageRead += math.ceil(len(mem)/pSize)
		out.write(mem)
		pageWrite += math.ceil(len(mem)/pSize)
	out.close()
	#Increment pass after pass 0 finishes and populate the meta list to prepare for pass 1+
	passes += 1
	for num in range(0, fSize, memSize):
		meta.append(num)
	#This will continue to run passes until there's only the 0 index in meta, meaning there's only 1 chunk to merge and the sort is complete
	while (len(meta) != 1):
		inp = open(output, "rb")
		temp = open("temp.db", "wb")
		#How many runs are needed to run all the chunks
		runs = math.ceil(len(meta)/(bPage-1))
		for num in range(runs):
			#Run merge on each batch of chunks of data
			#Merge takes bPage number of pages, assuming python cleans up the bytearray used in pass 0 then only bPages are used
			#At most bPage - 1 number of pages is passed through since 1 page is used for output buffer
			merge(inp, temp, meta[num*(bPage-1):min((num+1)*(bPage-1), len(meta))], pSize, fSize, field)
		passes += 1
		#Depopulate and make a new metalist, exponentially increase chunk size
		meta = []
		for num in range(0, fSize, int(math.pow((bPage-1), passes)*memSize)):
			meta.append(num)
		temp.close()
		inp.close()
		#Set the temp file to be the file being read in the next pass, or outfile if no more passes.
		os.remove(output)
		os.rename("temp.db", output)
	print("number of pages read: " + str(pageRead) + "\nnumber of pages written: " + str(
		pageWrite) + "\nnumber of passes: " + str(passes))
	#Reset global variables for batch runs
	passes = 0
	pageRead = 0
	pageWrite = 0

#sortDB("names.db", "out.db", 10, 1024, 1)
pgSize = [512, 1024, 2048]
pgNum = [3, 10, 20, 50, 100, 200, 500, 1000, 5000, 10000]

for elem in pgSize:
	for num in pgNum:
		print("\nResults for input: " + "page size: " + str(elem) + " page num: " + str(num) + "\n")
		sortDB("names.db", "out.db", num, elem, 1)








