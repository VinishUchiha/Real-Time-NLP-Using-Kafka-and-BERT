from confluent_kafka.admin import AdminClient, NewTopic
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic_name', type=str, required=True, help="kafka topic name we want to create")
    parser.add_argument('--kafka_bootstrap_servers', type=str,default='localhost', help="kafka bootstrap servers address")
    parser.add_argument('--num_partitions', type=int, default=1, help="number of partitions")
    parser.add_argument('--replication_factor', type=int,default=1, help="replication factor")
    args = parser.parse_args()
    admin = AdminClient({'bootstrap.servers': args.kafka_bootstrap_servers})
    topic = NewTopic(args.topic_name, num_partitions=args.num_partitions, replication_factor=args.replication_factor)
    admin.create_topics([topic])