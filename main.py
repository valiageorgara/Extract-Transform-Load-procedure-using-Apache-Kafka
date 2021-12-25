import os
import csv

from Flask import app_flask
from MySQL import insertToMySQL
from Neo4j import insertToNeo4j

if __name__ == '__main__':

    with open('products.csv', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    first = 0
    products = []
    counter = 0
    for i in data:
        # print(i[0])
        if i[0] != 'Smartphone':
            products.append(i[0])
            counter += 1
        if counter == 100:
            break

    # Connect to MySql database, create table Products and insert 50 products
    insertToMySQL(products)
    # Connect to Neo4j database, create users and relationships and insert 15 users
    insertToNeo4j()

    # delete existing topics
    os.system("sudo -S docker exec kafka tmp/deleteTopic.sh")

    # start the consumer
    os.system("gnome-terminal -- python Consumer.py")

    # start the producers
    os.system("gnome-terminal -- python ERProducer.py")
    os.system("gnome-terminal -- python GraphProducer.py")

    # start the flask
    app_flask.run()
