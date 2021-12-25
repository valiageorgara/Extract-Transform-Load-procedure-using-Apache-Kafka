import random

import mysql.connector
from mysql.connector import errorcode


def insertToMySQL(products):
    try:
        cnx = mysql.connector.connect(user='root', password='filarakia6',
                                      database='productsSchema')
        cursor = cnx.cursor()
        table = ("CREATE TABLE `productsSchema`.`Products` ("
                 "`productID` INT NOT NULL,"
                 "`productName` LONGTEXT NULL,"
                 "`price` INT NULL,"
                 "`currency` VARCHAR(45) NULL,"
                 "`reviews` INT NULL,"
                 " PRIMARY KEY (`productID`),"
                 "UNIQUE INDEX `productID_UNIQUE` (`productID` ASC) VISIBLE);")

        try:
            print("Creating table Products")
            cursor.execute(table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        try:
            for number in range(1, 51):
                addProduct = ("INSERT INTO Products "
                              "(productID, productName, price, currency, reviews) "
                              "VALUES (%s, %s, %s, %s, %s)")
                product_name = products[number]
                price_pr = random.randint(300, 1000)
                currency_pr = '€'
                reviews_pr = random.randint(5463, 8236408)
                values = (number, product_name, price_pr, currency_pr, reviews_pr)
                # print(addProduct)
                cursor.execute(addProduct, values)
                cnx.commit()
            print("Insert was done successfully")

        except mysql.connector.Error as error:
            print("Failed to insert into MySQL table {}".format(error))

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor.close()
        cnx.close()
        print("MySQL connection is closed")
