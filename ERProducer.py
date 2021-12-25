import json
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
import mysql.connector
from mysql.connector import errorcode
try:
    cnx = mysql.connector.connect(user='root', password='filarakia6',
                                  database='productsSchema')
    cursor = cnx.cursor()

    selectALL = "SELECT * FROM Products;"
    cursor.execute(selectALL)
    row_headers = [x[0] for x in cursor.description]
    rv = cursor.fetchall()
    json_data = []
    for result in rv:
        json_data.append(dict(zip(row_headers, result)))

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))

    for i in range(50):
        print('Sending data: ', json_data[i])
        future = producer.send('products-topic', json_data[i])
        sleep(2)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)

            # Successful result returns assigned partition and offset
            print(record_metadata.topic)
            print(record_metadata.partition)
            print(record_metadata.offset)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            print('[ERROR] ' + e.__str__())

    # block until all async messages are sent
    producer.flush()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
