import sqlite3

conn = sqlite3.connect('database.db')
print("opend")

conn.execute('DROP TABLE IF EXISTS users')
conn.execute('CREATE TABLE users (firstname TEXT, lastname TEXT, email TEXT, username TEXT PRIMARYKEY ,password TEXT)')
print("Table created successfully")
conn.close()