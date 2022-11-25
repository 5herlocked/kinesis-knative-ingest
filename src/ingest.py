import json
import time
import boto3
import requests
import multiprocessing as mp
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured


def interrupt_handler(sig_int, context):
    global running
    running = False
    pass


def fetch_and_censor(shard, pipe):
    kinesis_data = boto3.Session(region_name='us-east-1').client('kinesis')
    
    context = shard["ctx"]
    
    try:
        iterator = kinesis_data.get_shard_iterator(
            StreamName=context["data_stream"],
            ShardId=shard["ShardId"],
            ShardIteratorType='TRIM_HORIZON'
        )["ShardIterator"]
        print(iterator)
    except Exception as e:
        print("Exception while getting shard iterator: ", e)
        return

    failed_iter_duration = 0
    while iterator is not None:
        try:
            records = kinesis_data.get_records(
                ShardIterator=iterator
            )
            iterator = records["NextShardIterator"]

            for record in records["Records"]:
                payload = json.loads(record["Data"])
                if len(payload["FaceSearchResponse"]) == 0:
                    continue
                pipe.put(payload)
            
            # implemented exponential backoff
            failed_iter_duration = 0
        except Exception as e:
            failed_iter_duration += 1
            print("Exception getting records: ", e)

        time.sleep(1 * failed_iter_duration)
    pass


def consume_kinesis_shards(local_data_stream):
    global running, sink_url
    
    session = boto3.Session(region_name='us-east-1')
    
    # TODO: Figure out how to use RBAC and integrate it with IAM
    # to get access to kinesis data streams
    
    pool = mp.Pool()
    m = mp.Manager()
    q = m.Queue()
    
    kinesis_data = session.client('kinesis')
    shards = kinesis_data.list_shards(
        StreamName=local_data_stream
    )["Shards"]
    
    ctx = {
        "data_stream": local_data_stream,
    }
    
    attributes = {
        "type": "com.kinesis.ingester",
        "source": "/cloudevents/kinesis/record",
    }
    
    results = {}
    
    for shard in shards:
        shard["ctx"] = ctx
        
        results[shard["ShardId"]] = pool.apply_async(fetch_and_censor, args=(shard, q))
        
    # don't do anything till we actually have something in the queeu
    while q.empty():
        # prevent spinlock
        time.sleep(0.5)
        continue
        
    while running:
        # Emit CloudEvents here so that KNative can pass them to the correct
        data = q.get()
        event = CloudEvent(attributes, data)
        
        headers, body = to_structured(event)
        
        # Dispatch
        requests.post(sink_url, data=body, headers=headers)
        
        # prevent spinlock
        time.sleep(0.5)
        pass

    pool.terminate()
    pool.join()
    pass


if __name__ == '__main__':
    import os
    
    data_stream = os.environ['DATA_STREAM']
    region = os.environ['AWS_DEFAULT_REGION']
    sink_url = os.environ['K_SINK']
    
    running = True

    import signal

    signal.signal(signal.SIGTERM, interrupt_handler)
    signal.signal(signal.SIGINT, interrupt_handler)
    
    consume_kinesis_shards(data_stream)
    