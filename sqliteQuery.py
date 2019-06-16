from io import FileIO
from struct import *
from timeit import default_timer as timer

sqlite_file = open("dbA.db", "rb")

#Sets up global variables for statistics
#only totalTimeA and pageA are used for database 1 and 2 since they all access the same pages
totalTimeA = 0
totalTimeB = 0
totalTimeC = 0
pageA = [0, 0, 0, 0, 0]
pageB = [0, 0, 0, 0, 0]
pageC = [0, 0, 0, 0, 0]

#This function converts the serial type number to the size and returns it as an int
def typeToSize(x):
    if x <= 4:
        return x
    elif x == 5:
        return 6
    elif x <= 7:
        return 8
    elif x >= 12 and x%2 == 0:
        return int((x-12)/2)
    elif x >= 13 and x%2 == 1:
        return int((x-13)/2)

#Resets the global variables
def clearGlobals():
    global totalTimeA
    global totalTimeB
    global totalTimeC
    global pageA
    global pageB
    global pageC
    totalTimeA = 0
    totalTimeB = 0
    totalTimeC = 0
    pageA = [0, 0, 0, 0, 0]
    pageB = [0, 0, 0, 0, 0]
    pageC = [0, 0, 0, 0, 0]

#This function reads a series of bytes and calculates the varint
#Returns a tuple where tuple[0] is the varint itself, and tuple[1] is how many bytes the varint was
def readVarInt(bytearray):
    string = ''
    count = 0
    for num in bytearray:
        calc = f'{num:08b}'
        if count == 8:
            string += calc
        else:
            string += calc[1:]
        count+=1
        if calc[0]==('0'):
            break
    return (int(string, 2), count)

#This function reads a series of bytes and returns a list of all the varints contained in the bytearray
def readVarSeq(bytearray):
    retList = []
    count = 0
    while count != len(bytearray):
        temp = readVarInt(bytearray[count:])
        retList.append(temp[0])
        count += temp[1]
    return retList

#This function returns the page number of the root page for the tree
#Must be supplied with the bytearray representing the first page of the database, which contains the sqlite_master table
def getRootPage(bytearray):
    pageSize = unpack('>h', bytearray[16:18])[0]
    start = unpack('>h', bytearray[105:107])[0]
    firstSkip = readVarInt(bytearray[start:start+9])[1]
    secondSkip = readVarInt(bytearray[start+firstSkip:start+9+firstSkip])[1]
    pStart = start+firstSkip+secondSkip
    pHeaderSize = readVarInt(bytearray[pStart:pStart+9])[0]
    records = readVarSeq(bytearray[pStart:pStart+pHeaderSize])
    recordStart = pStart + pHeaderSize
    return bytearray[int(recordStart + typeToSize(records[1]) + typeToSize(records[2]) + typeToSize(records[3]))]


#The main function that is being ran for q4 a, b, and c
#This is only for the first 2 database files
#Change the header size to 16kb for databse 2
def read_page():
    header = bytearray(4096)
    start = timer()
    sqlite_file.readinto(header)
    end = timer()
    global totalTimeA
    totalTimeA += (end-start)
    global pageA
    pageA[1] += 1
    print('header string: ' + header[0:15].decode('utf-8')) # note that this ignores the null byte at header[15]
    page_size = unpack('>h', header[16:18])[0]
    print('page_size = ' + str(page_size))
    print('write version: ' + str(header[18]))
    print('read version: ' + str(header[19]))
    print('reserved space: ' + str(header[20]))
    print('Maximum embedded payload fraction: ' + str(header[21]))
    print('Minimum embedded payload fraction: ' + str(header[22]))
    print('Leaf payload fraction: ' + str(header[23]))
    file_change_counter = unpack('>i', header[24:28])[0]
    print('File change counter: ' + str(file_change_counter))
    print('Number of pages in database: ' + str(unpack('>i', header[28:32])[0]))
    print("Root page number: " + str(unpack('>i', header[52:56])[0]))
    sqlite_version_number = unpack('>i', header[96:100])[0]
    print('SQLITE_VERSION_NUMBER: ' + str(sqlite_version_number))
    query = start_read(getRootPage(header), page_size)
    print('\nqueries for 4a with the first database: ')
    for item in query:
        if item[0] == -1:
            print(item[1:])
    print('\nqueries for 4b with the first database: ')
    for item in query:
        if item[0] == -2:
            print(item[1:])
    print('\nqueries for 4c with the first database: ')
    for item in query:
        if item[0] == -3:
            print(item[1:])
    print("\ntotal time in seconds is: " + str(totalTimeA) + " seconds")
    print("\naverage time per page is: " + str(totalTimeA/sum(pageA)) + " seconds")
    print("\ntotal amount of internal table pages read is: " + str(pageA[0]) + " pages")
    print("\ntotal amount of leaf table pages read is: " + str(pageA[1]) + " pages")
    print("\ntotal amount of overflow pages read is: " + str(pageA[4]) + " pages")


#This function checks if there's an overflow and sets the payload stored in the leaf page/overflow page for other functions
def if_overflow(pageSize, payload):
    x = pageSize-35
    m = ((pageSize-12)*32/255)-23
    k = m+((payload-m)%(pageSize-4))
    if (payload > x and k <= x):
        return (k, payload-k)
    else:
        return (m, payload-m)

#This reads the overflow page recursively and returns the bytearray stored in the overflow pages
def read_overflow(pNum, pageSize, payload):
    start = timer()
    sqlite_file.seek((pNum-1)*pageSize)
    page = bytearray(pageSize)
    end = timer()
    global totalTimeA
    global pageA
    totalTimeA += (end-start)
    pageA[4] += 1
    sqlite_file.readinto(page)
    if unpack('>i', page[0:4])[0] == 0:
        return page[4:4+payload]
    else:
        return page[4:pageSize] + read_overflow(unpack('>i', page[0:4])[0], pageSize, payload)

#Takes the list of serial codes and the payload area, returns a list of the decoded payload items
#Hardcoded for this database since all emp_ids are 3 bytes and everything else is text
def decodeBytes(list, arr):
    offset = 0
    newList = []
    for item in list:
        if item >= 13 and item%2 == 1:
            newList.append(arr[offset:offset+typeToSize(item)].decode('utf-8'))
            offset += typeToSize(item)
        elif item == 3:
            newList.append(unpack('>i', bytearray(b'\x00') + arr[offset:offset+3])[0])
            offset += 3
        else:
            return item

    return newList


#This function starts recursively reading the root page of the tree for table pages
#Returns a list containg sublists for queries on 4a, 4b, and 4c
#Can be used for both the first and the second database
def start_read(pNum, pageSize):
    page = bytearray(pageSize)
    global totalTimeA
    global pageA
    start = timer()
    sqlite_file.seek((pNum-1)*pageSize)
    sqlite_file.readinto(page)
    end = timer()
    totalTimeA += (end-start)
    #If interior, recursively call on right-most pointer and left child
    if page[0] == 5:
        pageA[0] += 1
        temp_left = []
        temp_right = []
        num_cells = unpack('>h', page[3:5])[0]
        right_pointer = unpack('>i', page[8:12])[0]
        temp_right += start_read(right_pointer, pageSize)
        for num in range(num_cells):
            cell_start = unpack('>h', page[12+num*2:14+num*2])[0]
            child_ptr = unpack('>i', page[cell_start:cell_start+4])[0]
            key = readVarInt(page[cell_start+4:cell_start+13])
            temp_left += start_read(child_ptr, pageSize)
        return temp_left + temp_right

    #Unpacks data if leaf node, compiles into lists of data and return as queries.
    elif page[0] == 13:
        pageA[1] += 1
        overflow = 0
        query = []
        num_cells = unpack('>h', page[3:5])[0]
        for num in range(num_cells):
            cell_start = unpack('>h', page[8+num*2:10+num*2])[0]
            payload = readVarInt(page[cell_start:cell_start+9])[0]
            if payload > pageSize-35:
                t = if_overflow(pageSize, payload)
                payload = t[0]
                overflow = t[1]
            p_offset = cell_start + readVarInt(page[cell_start:cell_start+9])[1]
            row_id = readVarInt(page[p_offset:p_offset+9])[0]
            rowid_offset = p_offset + readVarInt(page[p_offset:p_offset+9])[1]
            if overflow > 0:
                rest = page[rowid_offset:rowid_offset+payload] + read_overflow(unpack('>i', page[rowid_offset+payload:rowid_offset+payload+4])[0], pageSize, payload)
            else:
                rest = page[rowid_offset:rowid_offset+payload]
            cell_header = readVarInt(rest[0:9])[0]
            headers = readVarSeq(rest[0:cell_header])
            list = decodeBytes(headers[1:], rest[cell_header:])
#Comment the next 2 lines of code out to stop 4a from running
            if list[4] == 'Rowe':
                query.append([-1, list[0], list[2], list[3], list[4]])
#Comment the next 2 lines of code out to stop 4b from running
            if list[0] == 181162:
                query.append([-2, list[2], list[3], list[4]])
#Comment the next 2 lines of code out to stop 4c from running
            if list[0] >= 171800 and list[0] <= 171899:
                query.append([-3, list[0], list[2], list[3], list[4]])
        return query

    else:
        return -1

#This is the main function that is called
read_page()


