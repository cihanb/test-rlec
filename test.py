import sys
import time
import redis

# START HERE
retry_counter = 0
downtime = 0

key_prefix = sys.argv[1]
redis_host = sys.argv[2]
redis_port = sys.argv[3]

while (retry_counter < 100):
    try:
        pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=0)
        conn = redis.Redis(connection_pool=pool)
        pipe = conn.pipeline(transaction=False)
        while (retry_counter < 100):
            try:
                for i in range(1,40000):
                    t0 = time.clock()
                    pipe.multi()
                    for j in range (0,10):
                        # print ("key:", str(i) + "-" + str(j) + "{"+ str(target_shard) +"}")
                        pipe.set(str(key_prefix) + "-" + str(j) + "{"+ str(i) +"}",
                            {'a1': "1", 'a2': "".zfill(100)})
                        # conn.watch(str(key_prefix) + "-" + str(j) + "{"+ str(i) +"}")
                        # pipe.get(str(key_prefix) + "-" + str(j) + "{"+ str(i) +"}")
                    pipe.execute()
                    t1 = time.clock()
                    print("Last batch execution time: {:6.3f} ms - AVG Command execution time in batch: {:6.3f} ms"
                        .format(((t1 - t0) * 1000), ((t1 - t0) * 1000)/10))
            except redis.ConnectionError:
                if (downtime == 0) or ((time.clock() - downtime) > 1000):
                    downtime = time.clock()
                    print("Connection Failed. Downtime started: {:6.3f} secs".format(time.clock() - downtime))
                    if (retry_counter >= 100):
                        raise NameError("Exhausted all retries.")
                    else:
                        retry_counter = retry_counter + 1
                        conn = redis.Redis(connection_pool=pool)
                        pipe = conn.pipeline(transaction=False)
            # except redis.ResponseError :
            #     retry_counter = 100
            #     print ("Unknown Error.")
            #     break
    except redis.ConnectionError:
                if (downtime == 0) or ((time.clock() - downtime) > 1000):
                    downtime = time.clock()
                    print("Connection Failed. Downtime started: {:6.3f} secs".format(time.clock() - downtime))
                    if (retry_counter >= 100):
                        raise NameError("Exhausted all retries.")
                    else:
                        retry_counter = retry_counter + 1
                        pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=0)
                        conn = redis.Redis(connection_pool=pool)
    else:
        print("DONE: inserted total items: ")