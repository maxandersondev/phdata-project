import mysql.connector
from datetime import datetime

import re


class DataWriter():
    def __init__(self):
        try:

            self.cnx = mysql.connector.connect(user='max', password='Mander5150!',
                              host='127.0.0.1',
                              database='phdata')
            self.cur = self.cnx.cursor()
            print("Connected to DB")
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))

    def __del__(self):
        self.cnx.close()
        print("DB connection closed")

    def show_version(self):
        self.cur.execute("SELECT VERSION()")
        print("Database version : %s " % self.cur.fetchone())

    def insertRecord(self, message):
        add_data = ("INSERT INTO apacheLogs "
                    "(ip, logData, accessTime) "
                    "VALUES (%s, %s, %s)")


        regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "-" "(.*?)"'
        logList = re.match(regex, message).groups()
        myDateTime = datetime.strptime(logList[1], "%d/%b/%Y:%H:%M:%S %z")
        formattedDate = myDateTime.strftime('%Y-%m-%d %H:%M:%S')
        insertList = (logList[0], message, formattedDate)
        print(insertList)
        self.cur.execute(add_data, insertList)
        self.cnx.commit()

    def getDDoSRecords(self, numRecords, hitCountThresh, spanInMinutes):
        #use something similar to this in real world situation of streaming log items
        #select_data = "select ip, count(ip) as ipCount from apacheLogs where accessTime > now() - interval %s second group by ip order by ipCount desc ;"
        select_data = "select ip, count(ip) as ipCount from apacheLogs group by ip having count(ip) >= {1} order by ipCount desc limit {0};".format(numRecords, hitCountThresh)
        self.cur.execute(select_data)
        return self.cur.fetchall()

    def getAllInfo(self, ipList):
        print("Generating all info file")
        print(ipList)
        #select_data = "select * from apacheLogs where ip in ({0})".format(",".join('?' * len(ipList)))
        select_data = "select * from apacheLogs where ip in ("
        for x in ipList:
            select_data = select_data + "'" + x + "',"
        select_data = select_data.rstrip(",") + ")"
        print(select_data)
        self.cur.execute(select_data)
        return self.cur.fetchall()


if __name__ == '__main__':  # If it's executed like a script (not imported)
    db = DataWriter()
    db.show_version()
    line = '209.112.63.162 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; {1C69E7AA-C14E-200E-5A77-8EAB2D667A07})"'

    splitLine = line.split(' ', 1)

    db.insertRecord(splitLine)
    del db




