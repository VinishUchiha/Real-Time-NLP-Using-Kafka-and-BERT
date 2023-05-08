# Real-Time NLP using Kafka and BERT


At First, Create the New Kafka topic

```bash
python kafka_topic_creator.py --topic_name twitter_data --kafka_bootstrap_servers localhost --num_partitions 2 --replication_factor 1
```

After Creating the topic, We need to start the producer and feed the tweet data one by one with the sleep time of 10ms. Copy the downloaded kaggle data to the data folder

```bash
python tweet_producer.py --data_path ./data/tweets_data.csv --topic_name twitter_data --kafka_bootstrap_servers localhost --sleep 0.01
```
