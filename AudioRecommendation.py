from pyspark import SparkConf,SparkContext
conf=SparkConf().setMaster("local").setAppName("AudioRecommender")
sc=SparkContext(conf=conf)
from random import randrange
from operator import itemgetter
from pyspark.mllib.recommendation import ALS, Rating

base="s3://shanaxmiku/csds-spark-emr/"
rawUserArtistData = sc.textFile(base + "user_artist_data.txt").cache()
rawArtistData = sc.textFile(base + "artist_data.txt").cache()
rawArtistAlias = sc.textFile(base + "artist_alias.txt").cache()

def _parseArtistAlias(line):
    fields = line.split('\t')
    try:
        artist = int(fields[0])
        alias = int(fields[1])
        return (artist,alias)
    except:
        return (None,None)
artistAlias=rawArtistAlias.map(_parseArtistAlias).collectAsMap()

def _parseUserArtist(line):
    fields = line.split(' ')
    user = int(fields[0])
    artist = int(fields[1])
    return (user,artist)
UserArtistData = rawUserArtistData.map(_parseUserArtist)

def _parseArtistData(line):
    fields = line.split('\t')
    try:
        artist_id = int(fields[0])#id is reserved in Python
        name = str(fields[1].strip())
        return (artist_id,name)
    except:
        return (None,None)
ArtistData=rawArtistData.map(_parseArtistData)  

def buildCounts(rawUserArtistData, bArtistAlias):
    def getArtistRating(line):
        (userID, artistID, count) = map(lambda x: int(x), line.split(' '))
        try:
            finalArtistID = bArtistAlias.value[artistID]
        except KeyError:
            finalArtistID = artistID
        return Rating(userID, finalArtistID, count)

    return rawUserArtistData.map(lambda line: getArtistRating(line))
bArtistAlias = sc.broadcast(artistAlias)
trainData=buildCounts(rawUserArtistData,bArtistAlias)
trainData.cache()

model=ALS.trainImplicit(ratings=trainData,rank=10,iterations=5,lambda_=0.01,alpha=1.0)

userID=2093760
recommendations=model.recommendProducts(userID,10)
recommendedProductIDs = map(lambda rec: rec.product, recommendations)

stack=[]
for i in recommendedProductIDs:
    #print(i)
    stack.append(i)

recommendedArtists = ArtistData.filter(lambda artist: artist[0] in stack).collect()

print(recommendedArtists)

