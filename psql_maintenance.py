'''
Created on Feb 25, 2014

@author: nfergusn
'''

import psycopg2 as psql
import os
sys.path.append('$HOME/source')
from caeser import utils

params = utils.connection_info('localhost', db='db')
engine = utils.connect(**params)

db = psql.connect(params)

cursor = db.cursor()

select = """select table_name from information_schema.tables where table_schema = 'tiger'"""
index = """create index idx_{0}_geom on tiger.{0} using gist(geom);"""
cursor.execute(select)

tables = cursor.fetchall()

def create_index():
    index = """create index idx_{0}_geom on tiger.{0} using gist(geom);"""
    
for t in tables:
    print t[0]
    cursor.execute("""select UpdateGeometrySRID('tiger', '{0}', 'geom', 4326);""".format(t[0]))
#     print t[0]
#     cursor.execute(index.format(t[0]))