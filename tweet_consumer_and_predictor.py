from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import torch
import numpy as np
from scipy.special import softmax
from confluent_kafka import Consumer, KafkaException
import json
import time
from pymongo import MongoClient
from dotenv import dotenv_values
import argparse

# load environmental variable as dict
config = dotenv_values(".env")

# MongoDB setup
conn_str = config['MONGODB_CONN_STR']
client = MongoClient(conn_str)

# Preprocessing step used during training tweets data
def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)

# Move the model to gpu if available and set eval mode
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device).eval()

def predict(text):
    if isinstance(text, str):
        text = preprocess(text)
    elif isinstance(text, list):
        text = [preprocess(txt) for txt in text]
    encoded_input = tokenizer(text, return_tensors='pt', padding=True, truncation=True).to(device)
    output = model(**encoded_input)
    batch_scores = output.logits.detach().cpu().numpy()
    rankings = [np.argsort(softmax(scores)) for scores in batch_scores]
    predicted_ids = [ranking[::-1][0] for ranking in rankings]
    predicted_labels = [config.id2label[id] for id in predicted_ids]
    return predicted_ids, predicted_labels

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic_name', type=str, required=True, help="kafka topic name we want to consume data")
    parser.add_argument('--kafka_bootstrap_servers', type=str,default='localhost', help="kafka bootstrap servers address")
    parser.add_argument('--consumer_group_id', type=str, default='g1', help="kafka consumer group id")
    parser.add_argument('--offset', type=str, default='latest', help="path of the tweet data")
    parser.add_argument('--mongodb_database', type=str, default='StreamingNLP', help="mongodb database name to write the results")
    parser.add_argument('--mongodb_collection_name', type=str, default='sentiments-gpu-plain-deploy-test', help="mongodb collection name to write the results")
    args = parser.parse_args()

    db = client.get_database(args.mongodb_database)
    collection = db.get_collection(args.mongodb_collection_name)

    consumer = Consumer(
        {
            'bootstrap.servers': args.kafka_bootstrap_servers,
            'group.id': args.consumer_group_id,
            'auto.offset.reset': args.offset
        }
    )
    consumer.subscribe([args.topic_name])

    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf8')
                record = json.loads(val)
                partition = event.partition()
                print(f'Received: {val} from partition {partition}')
                text = record['text']
                pred_id, pred_label = predict(text)
                record['predicted_id'] = int(pred_id[0])
                record['predicted_label'] = pred_label[0]
                record['predicted_at'] = time.time()
                collection.insert_one(record)
                consumer.commit(event)
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        consumer.close()