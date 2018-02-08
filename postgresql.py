'''
Created on Oct 21, 2013

@author: nfergusn
'''

import psycopg2 as pg
import os
sys.path.append('$HOME/source')
from caeser import utils

params = utils.connection_info('localhost', db='db')
engine = utils.connect(**params)

directory = r'e:\data\census\acs\tables'

database = raw_input('Database Name: ')

db = pg.connect(**params).format(database)

cursor = db.cursor()

if census_tbl == 'ACS':
    pre = 

def exe(f, tbl):
        print 'Inserting values for ', tbl, '\n'
        print f, '\n'
        query = """COPY {0} FROM {1} %s DELIMITER ',';""".format(f, tbl) 
        print query, '\n'
        cursor.execute(query % (f, tbl))#(query, (f, tbl))
        db.commit()
        print 'Successfully added ', tbl, '\n'


for docRoot, dirs, files in os.walk(directory):
    for f in files:
        #estimate f
        f = os.path.join(docRoot, f)
        if f[0] == 'e':
            tbl = 'acs_11_5yr.seq' + f[8:12]
            exe(f, tbl)                  
        #margin of error f    
        elif f[0] == 'm':
            tbl = 'acs_11_5yr.seq' + f[8:12] + '_moe'
            exe(f, tbl)         
        #geography f    
        elif f[0] == 'g' and f.endswith('csv'):
            tbl = 'acs_11_5yr.geoheader'
            exe(f, tbl) 

cursor.close()
db.close()
print 'Complete...'
            