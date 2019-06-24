import mysql.connector
from mysql.connector import errorcode


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

    def insertRecord(self, logList):
        add_data = ("INSERT INTO apacheLogs "
                    "(ip, logData) "
                    "VALUES (trim(%s), trim(%s))")
        self.cur.execute(add_data, logList)
        self.cnx.commit()


if __name__ == '__main__':  # If it's executed like a script (not imported)
    db = DataWriter()
    db.show_version()
    line = '209.112.63.162 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; {1C69E7AA-C14E-200E-5A77-8EAB2D667A07})"'

    splitLine = line.split(' ', 1)

    db.insertRecord(splitLine)
    del db




