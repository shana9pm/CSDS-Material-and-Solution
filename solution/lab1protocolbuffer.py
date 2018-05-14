import twitter_pb2



tweets=twitter_pb2.Tweets()

f=open("twitter.pb","rb")
tweets.ParseFromString(f.read())
f.close()

#Find the number of deleted messages in the dataset.
deleteCount=0
for delete in tweets.tweets:
  if delete.is_delete==True:
      deleteCount+=1
print "%d messages are deleted." %deleteCount

#Find the number of tweets that are replies to another tweet.
repliesToOther=0
for reply in tweets.tweets:
  if reply.HasField('insert'):
    if reply.insert.HasField('reply_to'):
      repliesToOther+=1
print "%d tweets are replies to another tweet" %repliesToOther

#Find the five user IDs (field name: uid) that have tweeted the most.
tweetsCountDict={}
for uidTweetCount in tweets.tweets:
  if uidTweetCount.HasField('insert'):
    print uidTweetCount.insert.created_at
    Tempuid=str(uidTweetCount.insert.uid)
    if tweetsCountDict.has_key(Tempuid)==False:
      tweetsCountDict[Tempuid]=1
    else:
      tweetsCountDict[Tempuid]+=1

print "Find the five user IDs (field name: uid) that have tweeted the most."

i=0
for key,value in sorted(tweetsCountDict.iteritems(),key=lambda (k,v): (v,k),reverse=True):
  if i<5:
    print "%s: %s" % (key,value)
    i+=1