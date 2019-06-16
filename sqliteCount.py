#Counts the max column length for each column

import csv

max = []
columns = []
count = 0
cur = 0
cycle = 0
with open("500000 Records.csv") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        for item in row:
            if cycle == 0:
                count += 1
            cur += 1
            if len(max) < count:
                max.append(len(str(item)))
                columns.append(item)
            else:
                if len(str(item)) >= max[cur-1]:
                    max[cur-1] = len(str(item))
        cur = 0
        cycle = 1
for num in range(len(max)):
    print(columns[num] + ": " + str(max[num]))
    print(len(max))
