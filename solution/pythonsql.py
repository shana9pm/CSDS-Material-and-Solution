import sqlite3 as lite
import sys
import datetime


def dealDateSQLite(string):
#input date from SQLite,output datatime in string
  if string==None:
    return
  templist=str(string).split(' ')
  stringv1=templist[1]+'-'+templist[2]+'-'+templist[5]
  fdate=datetime.datetime.strptime(stringv1,'%b-%d-%Y')

  return fdate.strftime('%b-%d-%Y')

def dealDateCsv(string):
#Input date from csv,output datetime in string
  fdate=datetime.datetime.strptime(string,'%m/%d/%Y %H:%M')
  return fdate.strftime('%b-%d-%Y')

con = lite.connect('mytwitter.db')

with con:    
    
  cur = con.cursor()

  cur.execute("SELECT created_at,id FROM tweets;")
  sqldate = cur.fetchall()


  for row in sqldate:
    tempdate=row[0]
    tempid = row[1]
    if tempdate:

      datenew = dealDateSQLite(tempdate)
      cur.execute("update tweets set created_at = " + '\''
                  + datenew + '\'' + " where id = " + str(tempid) + ";")

# update date in tweet to standardize type

  cur.execute("select DateUTC from weather;")
  csvdate=cur.fetchall()

  for row in csvdate:
    print row
    # datenew=dealDateCsv(row)
    # cur.execute("update weather set DateUTC = "+ "\'"+datenew+"\'"+" where DateUTC ="
    #           +row+";")

#update date in weather to standardized type
