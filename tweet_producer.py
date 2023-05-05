from confluent_kafka import Producer
import pandas as pd
import time
import json
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, required=True, help="path of the tweet data")
    parser.add_argument('--topic_name', type=str, required=True, help="kafka topic name we want to feed data")
    parser.add_argument('--kafka_bootstrap_servers', type=str,default='localhost', help="kafka bootstrap servers address")
    parser.add_argument('--sleep', type=float, default=0.01, help="sleep time for each iteration")
    args = parser.parse_args()
    
    df = pd.read_csv(args.data_path,header=None, encoding='latin-1') #'tweets_sentiments/tweets_data.csv'
    df.columns = ['target','id','date', 'flag', 'user', 'text']
    records = df.to_dict(orient='records')
    #'twitter_data_batched_1ms'
    producer = Producer({'bootstrap.servers':args.kafka_bootstrap_servers})
    count = 1
    for record in records:
        record['produced_at'] = time.time()
        producer.produce(args.topic_name, json.dumps(record).encode('utf-8'), 
                         callback=lambda err, msg: print(f"Message delivered to Topic: {msg.topic()}. Total msg delivered: {count}"))
        time.sleep(args.sleep)
        producer.flush()
        count+=1