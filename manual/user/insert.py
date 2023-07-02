import pymysql
import pandas as pd

def connect_database():
    try:
        return pymysql.connect(host='localhost', user='root', password='root',db='yeardream',charset='utf8')
    except Exception as e:
        raise e

crate_table_user = """
CREATE TABLE IF NOT EXISTS users(
	id VARCHAR(20) PRIMARY KEY, 
	name VARCHAR(50) NOT NULL,
	position VARCHAR(10) NOT NULL,
	email VARCHAR(100) UNIQUE
)
"""

conn = connect_database()
cursor = conn.cursor()

cursor.execute(crate_table_user)
cursor.fetchall()
conn.commit()

df = pd.read_excel('이어드림_유저명단.xlsx')

for index, row in df.iterrows():
    cursor.execute("INSERT INTO users(id, name, position, email) VALUES (%s, %s, %s, %s)",
                (row['유니크 아이디'], row['실제이름'], row['직업'], row['이메일'])
    )

cursor.fetchall()
conn.commit()



