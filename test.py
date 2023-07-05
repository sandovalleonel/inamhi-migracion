import mysql.connector
host = 'localhost'
user = 'leonel'
password = 'admin'
database = 'mch'
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
