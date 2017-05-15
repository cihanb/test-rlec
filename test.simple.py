import sys
import time
import redis

# START HERE
retry_counter = 0
downtime = 0

key_prefix = sys.argv[1]
redis_host = sys.argv[2]
redis_port = sys.argv[3]

max_ops_latency = -1
min_ops_latency = sys.maxsize
pipeline_size = 10
warmup = 50

while (retry_counter < 100):
    try:
        pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=0)
        conn = redis.Redis(connection_pool=pool)
        pipe = conn.pipeline(transaction=False)
        while (retry_counter < 100):
            try:
                for i in range(1,400):
                    
                    #start timings 
                    t0 = time.clock()

                    #SET WITH PIPELINE
                    # pipe.multi()
                    # for j in range (0,pipeline_size):
                    #     # print ("key:", str(i) + "-" + str(j) + "{"+ str(target_shard) +"}")
                    #     pipe.set(str(key_prefix) + "-" + str(j) + "{"+ str(i) +"}",
                    #         {'a1': "1", 'a2': "".zfill(100)})
                    # pipe.execute()

                    #MSET WITHOUT PIPELINE
                    value = "'a1': '1', 'a2':" + "".zfill(100)+ "}"
                    conn.mset({str(key_prefix) + "-0" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-1" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-2" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-3" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-4" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-5" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-6" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-7" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-8" + "{"+ str(i) +"}":
                            value,
                            str(key_prefix) + "-9" + "{"+ str(i) +"}":
                            value})

                    #capture timings
                    t1 = time.clock()

                    if (warmup < i):
                        curr_pipeline_latency = ((t1 - t0) * 1000)
                        curr_pipeline_avg_ops_latency = curr_pipeline_latency / pipeline_size

                        if (max_ops_latency < curr_pipeline_avg_ops_latency):
                            max_ops_latency = curr_pipeline_avg_ops_latency

                        if (min_ops_latency > curr_pipeline_avg_ops_latency):
                            min_ops_latency = curr_pipeline_avg_ops_latency

                        print(key_prefix + " - Last pipeline execution time: {:6.3f} ms - AVG Command execution time in pipeline: {:6.3f} ms"
                            .format(curr_pipeline_latency, curr_pipeline_avg_ops_latency))
                
                #finish the loop
                retry_counter = 100

                print(key_prefix + " - pipeline execution time: MAX {:6.3f} ms - MIN  {:6.3f} ms"
                        .format(max_ops_latency, min_ops_latency))

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
