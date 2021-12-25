# Extract Transform Load procedure

<a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/etl.png">
</a>


In the present project we will develop an Extract Transform Load (ETL) mechanism. The ETL mechanism will retrieve data from two Databases of different types, integrate them, process them and finally store them in a third database. A Rest service will display data via get request from the third database. The work will be implemented in Python 3. The two databases from which the data will be extracted will be MySQL and Neo4J and the database where the data will be loaded will be MongoDB. The rest service will be implemented with the Flask Framework. The data pipeline and integration will be implemented with Apache Kafka.
The operating system of this project will be Ubuntu 20.04.

This project has been developed by Vasiliki Georgara under the course Full Stack Programming by Harokopio University of Athens.


## Installation

Install Docker

```bash
 sudo apt-get update

 sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

 curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
 $echo \"deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

 sudo apt-get update

 sudo apt-get install docker-ce docker-ce-cli containerd.io
```
MySQL Workbench
```bash
sudo apt update
sudo apt install mysql-workbench
```
mysql-connector
```bash
pip install mysql-connector-python
```
Kafka
```bash
pip install kafka-python
conda install -c conda-forge kafka-python
```
Flask
```bash
pip install Flask
```
Postman
```bash
sudo snap install postman
```
PyCharm community edition
```bash
sudo snap install pycharm-community --classic
```

## Usage
docker compose command
```bash
sudo docker-compose up -d
```
docker-compose.yml
```python
# Use root/example as user/password credentials
version: '3.1'

services:

  mongo:
    image: mongo
    restart: always
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: example

  mongo-express:
    image: mongo-express
    restart: always
    ports:
      - "8081:8081"
    environment:
      ME_CONFIG_MONGODB_ADMINUSERNAME: root
      ME_CONFIG_MONGODB_ADMINPASSWORD: example
      ME_CONFIG_MONGODB_URL: mongodb://root:example@mongo:27017/

  mysql:
    image: mysql
    restart: always
    environment:Kaggle
      MYSQL_ROOT_PASSWORD: filarakia6 
    ports:
      - '3306:3306'
    expose:
      - '3306'

  neo4j:
    image: neo4j
    hostname: neo4j
    restart: always
    container_name: neo4j
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      NEO4J_AUTH: neo4j/test

  zookeeper:
    image: wurstmeister/zookeeper
    hostname: zookeeper
    restart: always
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: wurstmeister/kafka
    hostname: kafka
    restart: always
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181

```
## Runtime
First, we have downloaded from [Kaggle](https://www.kaggle.com/ankitkalauni/amazon-top-rated-smartphones-accessories-2021) a dataset for the Amazon Top Rated Smartphones & Accessories 2021 and we have extracted the first column of the dataset into a list.

Before any execution, all the topics are deleted, so that they start from offset 0, all the users are deleted from Neo4j, so new ones are created each time and the ETL starts from scratch.

We connect to the MySQL database, and we insert 50, 100 or more from the products, along with an ascending productID key, a random price in the range (300,1000), the currency(€) and a random review number.

<a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/products.png">
</a>
Then, we connect to the Neo4j database, and we insert a graph of users, with attributes such as name, productID (a list of random 1 to 5 productIDs, age, and date of purchase.

<a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/users.png">
</a>

We have two scripts for each producer. The ERProducer.py which sends messages to the products-topic. And the GraphProducer.py which sends messages to the users-topic.

The Consumer.py script connects to the MongoDB and starts accepting messages one-by one from the two topics. Each product message is stored in a list and each time a new product comes, we check a users list, to check if there is data that is not fused. If a user message arrives and the product exists in the products list, then the data is fused and inserted in the MongoDB. The consumer closes after 10 seconds of no messages and the consumer terminal closes after 10000 seconds.

At the end, we can send a GET request at http://localhost:5000/user/USERNAME and see their products.
<a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/postman.png">
</a>

## Acknowledgments
I've used this space to list resources I have fount helpful and would like to give credit to. 

[How to Set Up MySQL Database with Docker
](https://www.youtube.com/watch?v=kphq2TsVRIs&t=517s)

[Kafka installation using Docker and Docker Compose | Kafka | Docker | Docker Compose
](https://www.youtube.com/watch?v=WnlX7w4lHvM&t=1022s)

[Getting started with Apache Kafka in Python](https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05)

[Kafka-Python explained in 10 lines of code](https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1)

[more pythonic way to format a JSON string from a list of tuples](https://stackoverflow.com/questions/13761054/more-pythonic-way-to-format-a-json-string-from-a-list-of-tuples)

[Python converting mysql query result to json](https://stackoverflow.com/questions/43796423/python-converting-mysql-query-result-to-json)

[Python 3.7 compatibility issue #1566](https://github.com/dpkp/kafka-python/issues/1566)

[Using Neo4j from Python](https://neo4j.com/developer/python/)

[Neo4j Python Driver 4.4](https://neo4j.com/docs/api/python-driver/current/)

[How to create multiple producer and consumer in kafka using python](https://stackoverflow.com/questions/54932851/how-to-create-multiple-producer-and-consumer-in-kafka-using-python)

[How to Run Neo4j in Docker](https://www.youtube.com/watch?v=KMw282j6xNQ)

[How to generate a random date between two dates in Python](https://www.kite.com/python/answers/how-to-generate-a-random-date-between-two-dates-in-python)

[Python MongoDB - W3Schools](https://www.w3schools.com/python/python_mongodb_getstarted.asp)

[Convert PyMongo Cursor to JSON](https://www.geeksforgeeks.org/convert-pymongo-cursor-to-json/)

[$each](https://docs.mongodb.com/manual/reference/operator/update/each/#mongodb-update-up.-each)
