import myproto1_pb2



tweets=myproto1_pb2 .Tweets()


f=open("my.pb","rb")
tweets.ParseFromString(f.read())
f.close()

for x in tweets.tweets:
  if x.HasField("insert"):
    print x.insert.place.country_code
    print x.insert.created_at
    print x.insert.uid
    print x.insert.place.condition
