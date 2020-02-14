import dash
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
#from config import (user, password, dbname, host, port)
import os
import pandas as pd

host = "54.185.239.96"
port = "5432"
user = "anqi"
dbname = "test"
password = "1234"

def ping_postgres_db(db_name):
    '''Tests whether the given PostgreSQL database is live.'''
    try:
        conn = psycopg2.connect(host = host,\
        	port=port,\
        	user = user, \
        	dbname = db_name, \
        	password = password, \
        	sslmode='require')
        conn.close()
        return True
    except:
        return False

def table_exists(table_name):
    sql_query = "SELECT EXISTS (SELECT 1 FROM "+ table_name + ");"
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(sql_query)
        return True
    except:
        return False

def createDropdownOptions(op):
    options = []
    for item in op:
        newOp = {"label":item[0], "value": item[0]}
        options.append(newOp)
    return options



print("Connecting to model-test database...")

connected_to_db = False
while not connected_to_db:
    connected_to_db = ping_postgres_db(dbname)
print("Connected to database successfully!")

conn = psycopg2.connect(host = host, \
		port=port, \
		user = user, \
		dbname = dbname, \
		password = password, \
		sslmode='require')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

cur = conn.cursor()
cur.execute("SELECT * FROM parking LIMIT 3;")
parking = pd.read_sql_query("SELECT * FROM parking LIMIT 3;", conn)
collisions = pd.read_sql_query("SELECT * FROM collisions LIMIT 3;", conn)

#cur.execute("SELECT distinct(Name) FROM amazon_reviews ")
name = cur.fetchall()
print(parking)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

app.layout = html.Div([
    dcc.Input(id='my-id', value='initial value', type='text'),
    html.Div(id='my-div')
])



if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
