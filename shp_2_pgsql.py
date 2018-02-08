'''
Created on Jan 28, 2014

@author: nfergusn

Loads TIGER shapefiles into postgresql databse


'''
import psycopg2 as psql
import os
sys.path.append('$HOME/source')
from caeser import utils

params = utils.connection_info('localhost', db='db')
engine = utils.connect(**params)

db = psql.connect(**params)

cursor = db.cursor()


def table_exists(table):
    '''
    table_exists(table)
    
    checks whether table exists to determine appropriate sql command to be applied in shp_2_db
    returns boolean value
    
    INPUT:
    table:
        postgres table
    '''
    
    cmd = """select exists(select * from information_schema.tables where table_name='{0}')""".format(table)
    cursor.execute(cmd)
    exists =  cursor.fetchone()[0]
    return exists

def shp_2_db(shp, table, exists):
    '''
    shp_2_db(shp, table, exists)
    
    loads current shapefile into postgres database using sph2pgsql application
    if table exists, it executes the application using an append command (-a) otherwise it creates a new table
    and absorbs its schema
    
    parameters for shp2pgsql:
    -I: create index
    -a: append
    -h: host
    -U: username
    -d: database
    -I: create Index
    -s: SRID
    
    INPUT:
    shp: TIGER shapefile
    table: postgres table creaetd from Census directory structure during TIGER download and extraction
    exists: boolean value that determines whether a new table is created or data are appended to existing table
    '''
    if exists:
        print '\tAppending ', shp
        os.system('shp2pgsql -a -s 4326 {0} tiger.{1} Census | psql -h cpgis-geo -U postgres -d Census'.format(shp, table))
    else:
        print '\tCreating ', table
        os.system('shp2pgsql -s 4326 {0} Tiger.{1} Census | psql -h cpgis-geo -U postgres -d Census'.format(shp, table))

def add_shp(directory, table):
    '''
    add_shp(directory, table)
    
    iterator that walks through specified directory and passes each found shapefile to shp_2_db() for loading
    names for shapefiles are created from original TIGER name except for those that are specific to the census
    year (i.e. bg00, bg10)
    
    INPUT:
    directory: location that houses the TIGER data to be loaded
    table: name of the directory housing the data which will be used to create the resulting postgres table
    '''
    count = 1
    for root, dirs, files in os.walk(directory):   
        for f in files:
            if f.endswith('.shp'):
                print f
                shp = os.path.join(directory, f)
                #splits shapefile name to look for files that are from 2000 or 2010 census and renames accordingly
                if shp[-6:-4] in ['00', '10']:
                    psql_table = table.lower() + shp[-6:-4]
                else:
                    psql_table = table.lower()
                print '\tLoading ', shp, ' into ', psql_table 
                if table_exists(psql_table):
                    shp_2_db(shp, psql_table, True)
                else:
                    shp_2_db(shp, psql_table, False)
                count += 1
    print '\t', count, ' features loaded.'
