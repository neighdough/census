'''
Created on Oct 25, 2013

@author: nfergusn
'''
import psycopg2 as pg
import os
sys.path.append('$HOME/source')
from caeser import utils

params = utils.connection_info('localhost', db='db')
engine = utils.connect(**params)
directory = r'E:\Data\Census\DEC\2010\sf1'

db = pg.connect(**params)
cursor = db.cursor()

for docRoot, dirs, files in os.walk(directory):
    for f in files:
        with open(os.path.join(docRoot, f), 'r') as f_in:
            if f[2] == 'g':
                dict = {}
                for line in f_in.readlines():
                    dict[line]=(line[0:7],line[6:19], line[18:68],line[67:154],
                                line[153:165], line[164:172], line[171:201],
                                line[200:21])
                tbl = 'geoheader'
            else:
                tbl = 'sf1_' + f[2:7]
            cursor.copy_from(f_in, tbl, sep=',', null='')   
            db.commit()
            print 'Successfully added ', f, ' to ', tbl, '\n'
cursor.close()
db.close()
print 'Process complete'