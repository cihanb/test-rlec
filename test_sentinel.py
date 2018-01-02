import redis
from redis.sentinel import Sentinel

sentinel = Sentinel([('localhost', 8001)], socket_timeout=0.1)
db1 = sentinel.discover_master('db1')
print(db1)

r = redis.Redis(host=db1[0], port=db1[1], ssl=True, 
    ssl_keyfile='/root/user_private.key', 
    ssl_certfile='/root/user_cert.crt', 
    ssl_cert_reqs='required', 
    ssl_ca_certs='/root/proxy_cert.pem')
r.set("a",{'a1': "1", 'a5': "".zfill(100)})
print(r.info())