# Real-Time NLP using Kafka and BERT


At First, Create the New Kafka topic

```bash
python kafka_topic_creator.py --topic_name twitter_data --kafka_bootstrap_servers localhost --num_partitions 2 --replication_factor 1
```
