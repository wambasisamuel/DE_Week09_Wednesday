# DE_Week09_Wednesday

I set up a Kafka cluster comprised of one broker and a Zookeeper using a docker containers running on a DigitalOcean droplet

Deployment:
- Clone the reposiory
- Run `docker-compose up -d --build`
- Run the consumer: `docker exec pipeline python3 consumer.py`
- In another terminal, run the producer: `docker exec pipeline python3 producer.py`
- Switch back to the terminal where the consumer was running, you will see messages streaming in
