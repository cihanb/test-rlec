import sys
import redis

r = redis.StrictRedis(host="localhost", port=6379, db=0)

r.set (sys.argv[1], sys.argv[2])
print (r.get(sys.argv[1]))