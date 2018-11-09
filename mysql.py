import pandas

import pymysql

# Create a connection object

databaseServerIP = "127.0.0.1"  # IP address of the MySQL database server

databaseUserName = "root"  # User name of the database server

databaseUserPassword = ""  # Password for the database user

newDatabaseName = "NewDatabase"  # Name of the database that is to be created

charSet = "utf8mb4"  # Character set

cusrorType = pymysql.cursors.DictCursor

connectionInstance = pymysql.connect(host=databaseServerIP, user=databaseUserName, password=databaseUserPassword,

                                     charset=charSet, cursorclass=cusrorType)

try:

    # Create a cursor object

    cursorInsatnce = connectionInstance.cursor()

    # SQL Statement to create a database

    sqlStatement = "CREATE DATABASE " + newDatabaseName

    # Execute the create database SQL statment through the cursor instance

    cursorInsatnce.execute(sqlStatement)

    # SQL query string

    sqlQuery = "SHOW DATABASES"

    # Execute the sqlQuery

    cursorInsatnce.execute(sqlQuery)

    # Fetch all the rows

    databaseList = cursorInsatnce.fetchall()

    for datatbase in databaseList:
        print(datatbase)



except Exception as e:

    print("Exeception occured:{}".format(e))


df=pandas.read_csv('event-data-extract-v0.1.csv')


flag=1
for rr in df:

    item=rr.split('\t')
    print(len(item))
    if flag:
        tit=item
        break


db = pymysql.connect("localhost","root","","NewDatabase" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# Drop table if it already exist using execute() method.
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

# Create table as per requirement
sql = "CREATE TABLE EMPLOYEE ("
sqlInsert = "INSERT INTO EMPLOYEE ("


flag=1
for tt in tit:
    aa=tt.split('\"')
    if len(aa)>1:
        aa=aa[1]
    else:
        aa=aa[0]

    aa=aa.replace(".", "_")

    if flag:
        flag=0
        sql=sql+aa+" CHAR(20)"
        sqlInsert=sqlInsert+aa
    else:
        sql=sql+", "+aa+" CHAR(20)"
        sqlInsert=sqlInsert+", "+aa

sql=sql+")"
sqlInsert=sqlInsert+") VALUES ("

cursor.execute(sql)

flag=1
i=0
for rr in df:
    rr=df.loc[i].iat[0]
    i=i+1

    item = rr.split('\t')
    sql = sqlInsert

    ff = 1
    print(len(item))
    for aa in item:

        if ff:
            ff = 0
            sql = sql + "\""+aa+"\""

        else:
            sql = sql + ", " + aa

    sql=sql+")"
    print(sql)
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Commit your changes in the database
        db.commit()
    except:
        # Rollback in case there is any error
        db.rollback()
        print("adsfafds")

#
#
#
# disconnect from server
db.close()
#
#
#
#
# connectionInstance.close()

