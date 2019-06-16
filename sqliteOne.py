#This generates the db file for 3a

import csv, sqlite3

con = sqlite3.connect("dbA.db")
cur = con.cursor()

f = open('500000 Records.csv', 'r')
next(f, None)
reader = csv.reader(f)

#Setting up database 1 with page size, column names
cur.execute("PRAGMA page_size = 4096")
cur.execute("PRAGMA encoding = 'UTF-8'")
dropTable = "DROP TABLE IF EXISTS Employee"
createTable =  "CREATE TABLE Employee"\
               "('Emp ID' int, 'Name Prefix' char(11), 'First Name' char(11), 'Middle Initial' char(14), "\
               "'Last Name' char(13), 'Gender' char(6), 'E Mail' char(37), 'Fathers Name' char(25), "\
               "'Mothers Name' char(24), 'Mothers Maiden Name' char(20), 'Date of Birth' char(13), "\
               "'Time of Birth' char(13), 'Age in Yrs. ' char(11), 'Weight in Kgs. ' char(14), 'Date of Joining' char(15), "\
               "'Quarter of Joining' char(18), 'Half of Joining' char(15), 'Year of Joining' char(15), "\
               "'Month of Joining' char(16), 'Month Name of Joining' char(21), 'Short Month' char(11), "\
               "'Day of Joining' char(14), 'DOW of Joining' char(14), 'short DOW' char(9), 'Age in Company (Years)' char(22), "\
               "'Salary' char(6), 'Last % Hike' char(11), 'SSN' char(11), 'Phone No. ' char(12), 'Place Name' char(26), "\
               "'County' char(22), 'City' char(26), 'State' char(5), 'Zip' char(5), 'Region' char(9), "\
               "'User Name' char(15), 'Password' char(15))"

cur.execute(dropTable)
cur.execute(createTable)

for row in reader:
    cur.execute("INSERT INTO Employee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

con.commit()
con.close()