# Real-Time NLP using Kafka and BERT

This Repo contains the code for the article [When Kafka Meets BERT: A Realtime NLP using Kafka and BERT — Part 1](https://medium.com/@vinishuchiha_29660/when-kafka-meets-bert-a-realtime-nlp-using-kafka-and-bert-part-1-ee20a8226f02)

At First, Create the New Kafka topic

```bash
python kafka_topic_creator.py --topic_name twitter_data --kafka_bootstrap_servers localhost --num_partitions 2 --replication_factor 1
```

After Creating the topic, We need to start the producer and feed the tweet data one by one with the sleep time of 10ms. Copy the downloaded kaggle data to the data folder

```bash
python tweet_producer.py --data_path ./data/tweets_data.csv --topic_name twitter_data --kafka_bootstrap_servers localhost --sleep 0.01
```

Approach 1: Single Inference

```bash
python tweet_consumer_and_predictor.py --topic_name twitter_data --kafka_bootstrap_servers localhost --offset latest --mongodb_database analytics --mongodb_collection_name sentiments
```
Approach 2: Batched Inference

```bash
python tweet_consumer_and_predictor_batched.py --topic_name twitter_data --kafka_bootstrap_servers localhost --offset latest --batch_size 64 --mongodb_database analytics --mongodb_collection_name sentiments
```
