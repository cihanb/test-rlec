import redis

try:
    r = redis.Redis(host='127.0.0.1',port=12000,
        ssl=True,
        ssl_keyfile='/Users/cihan/certsuser_private.key',
        ssl_certfile='/Users/cihan/certsuser_cert.crt',
        ssl_cert_reqs='required',
        ssl_ca_certs='/Users/cihan/certsproxy_cert.pem')
    r.set("a",{'a1': "1", 'a2': "".zfill(100)})
    print(r.info())

except Exception as e:
    print(e)
