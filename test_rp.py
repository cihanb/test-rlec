import redis

r = redis.StrictRedis(host='localhost', port=12000, db=0)
r.set('key1', '123')
if (r.get('key1') == b'123'):
    print("DB TEST PASSED")
else:
    print("DB TEST FAILED")