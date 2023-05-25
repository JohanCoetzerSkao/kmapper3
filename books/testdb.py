#!/usr/bin/python
import mariadb

# connection parameters
conn_params= {
    "user" : "root",
    "password" : "fubar",
    "host" : "localhost",
    "database" : "kmapper3"
}

# Establish a connection
connection= mariadb.connect(**conn_params)

cursor = connection.cursor()

# Populate countries table  with some data
cursor.execute("INSERT INTO pages(page_file, page_title, page_url) VALUES (?,?,?) RETURNING page_id;",
               ("/tmp/foo.html", "State of affairs", "http://skao.int/foo.html"))

connection.commit()

# retrieve data
# cursor.execute("SELECT name, country_code, capital FROM countries")

# print content
row = cursor.fetchone()
print(*row, sep=' ')

# free resources
cursor.close()
connection.close()
