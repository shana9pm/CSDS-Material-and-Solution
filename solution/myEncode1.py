import csv
from json import loads
from itertools import imap
from myproto1_pb2 import *
import datetime
#My target is to creat a my.pb with only record of tweets created in 2013 && US && clear Condition(define in csv)


import google.protobuf.json_format

#row[12] is condition row[14] is date row 15 is countrycode

def dealDateProtocol(string):
#input date from protocol,output datatime in string
  templist=str(string).split(' ')
  stringv1=templist[1]+'-'+templist[2]+'-'+templist[5]
  fdate=datetime.datetime.strptime(stringv1,'%b-%d-%Y')

  return fdate.strftime('%b-%d-%Y')

def dealDateCsv(string):
#Input date from csv,output datetime in string
  fdate=datetime.datetime.strptime(string,'%m/%d/%Y %H:%M')
  return fdate.strftime('%b-%d-%Y')

def TestClear(string,dateList):
  outstring=dealDateProtocol(string)
  Logic=False
  for date in dateList:
    if outstring==date:
      Logic=True
  return Logic

with open('WeatherDataset.csv') as f:
  f_csv=csv.reader(f)
  DateInClear=[]
  for row in f_csv:
    if row[11]=='Clear':
      DateRight=dealDateCsv(row[13])
      DateInClear.append(DateRight)
#get a date list that is claer
f.close()

tweets = Tweets()
with file('twitter.json', 'r') as f:
  for line in imap(loads, f):
    tweet = tweets.tweets.add()
    if line.get('place',None):
      place = line['place']
      if place['country_code']=='US':
        if TestClear(line['created_at'],DateInClear):
          insert = tweet.insert
        insert.uid = line['user']['id']
        insert.truncated = line['truncated']
        insert.text = line['text']
        if line.get('in_reply_to_status_id', None):
          insert.reply_to = line['in_reply_to_status_id']
          insert.reply_to_name = line['in_reply_to_screen_name']
        insert.id = line['id']
        insert.favorite_count = line['favorite_count']
        insert.source = line['source']
        insert.retweeted = line['retweeted']
        if line.get('possibly_sensitive', None):
          insert.possibly_sensitive = line['possibly_sensitive']
          insert.lang = line['lang']
        insert.created_at = line['created_at']
        if line.get('coordinates', None):
          coords = line['coordinates']
          insert.coord.lat = coords['coordinates'][0]
          insert.coord.lon = coords['coordinates'][1]
        insert.filter_level = line['filter_level']

        if line.get('place', None):
          place = line['place']
          insert.place.url = place['url']
          insert.place.country = place['country']
          insert.place.country_code = place['country_code']
          insert.place.place_type = place['place_type']
          insert.place.id = place['id']
          insert.place.name = place['name']
          insert.place.condition='clear'
          if place.get('bounding_box', None):
            def add(pair):
              coord = insert.place.bounding_box.add()
              coord.lat = pair[0]
              coord.lon = pair[1]
            map(add, place['bounding_box']['coordinates'][0])



with file('my.pb', 'w') as f:
   f.write(tweets.SerializeToString())
