'''
Created on May 1, 2014

Builds Decennial Census Tables using provided Data Field Descriptors file extracted from SF1_Access2007.accdb downloaded from Census ftp.
Data Field Descriptors should be exported as csv and should include quotechar to delimit complete strings

TODO:

modify table creation to handle either Decennial or ACS

tbl_schema = {table number:[table comment, [start index, finish index, text file], [[field name, field comment]]}
DEC        = {row[2]: row[3], [start index, finish index, row[1] padded to length 5],[row[4], row[3]]}
ACS        = {row[2]: [start index, finish indes, row[3] padded to match pattern], [row[2] + row[4] padded to 3 digits, row[8]]}


@author: nfergusn, Center for Partnerships in GIS, University of Memphis
'''

import csv
import psycopg2 as psql
import os
import sys
sys.path.append('$HOME/source')
from caeser import utils

params = utils.connection_info('localhost', db='db')
engine = utils.connect(**params)
row_num = 0
seq_tbl = ''
tbl_schema = {}
db = psql.connect(**params)
db.set_client_encoding('UNICODE')
cursor = db.cursor()
#specify location of census data
census_directory = r'E:/Data/Census/DEC/2010'

# class Table:
#     
#     def __init__(self, tbl_name, field_name, field_comment, start_index, finish_index):
#         self.tbl_name = tbl_name
#         self.field_name = field.name
#         self.field_comment = field_comment
#         self.start_index = start_index
#         self.finish_index = finish_index
#     
#     def create_new_table(self):
            
def add_key(key):
    '''Creates tbl_schema key from first instance of table name'''
    tbl_schema[key] =  '' 

def key_exists(key):
    '''Checks for existence of table name/tbl_schema key'''
    return key in tbl_schema.keys() 
        
def build_temp_tables(tbl_schema):
    '''Creates the table in postgres using schema generated from DATA_FIELD_DESCRIPTORS
        
        tbl_schema: dictionary created from Data Field Descriptors file'''

    print 'Building tables...'
    sql_create = """CREATE TABLE temp_{0} ("fileid" varchar(6), "stusab" varchar(2), "chariter" varchar(3), "cifsn" varchar(3),
    "logrecno" numeric, "{1}" numeric); """
    for k, vals in tbl_schema.items():                
        fields = '" numeric, "'.join(v[0] for v in vals[2])
        cursor.execute(sql_create.format(k, fields))
        db.commit()
    '''insert data from sf1 files'''        
    insert_data(tbl_schema)

def schema_to_comment(tbl_schema):
    '''adds comments to postgresql table        
        tbl_schema: dictionary created from Data Field Descriptors file        
    '''
    print 'Adding comments to tables...'
    
    sql_comment = """\tCOMMENT ON TABLE "{0}": '{1}'; COMMENT ON COLUMN "{0}".{2}"""
    for k, vals in tbl_schema.items():              
        pair = '''COMMENT ON COLUMN "{0}".'''.join('"' + v[0].lower() + '"' + ' IS ' + "'"+ v[1].encode(encoding='ascii', errors='ignore') +"';" for v in vals[2]).format(k.lower())      
        print pair
        cursor.execute(sql_comment.format(k.lower(), vals[0], pair))
        db.commit()
    

def insert_data(tbl_schema):
    '''opens each state sequence file, splitting into individual tables using start and finish inidices
        created from Data Field Descriptors file 
        
        tbl_schema: dictionary created from Data Field Descriptors file'''
    
    print 'Inserting data...'
    dir_list = get_directory(os.path.join(census_directory,'sf1'))
    for d in dir_list:
        for k in tbl_schema.keys():
            vals = tbl_schema[k]
            file_prefix = d.split('\\')[-1]
            temp_out = []        
            with open(d +  '\\' + file_prefix + vals[1][2] + '2010.sf1', 'r') as f:
                reader = csv.reader(f, delimiter = ',', quotechar='"')                
                for line in reader:
                    out = line[0:6] + line[vals[1][0]:vals[1][1]]
                    temp_out.append(out)            
            with open(os.path.join(census_directory, 'csv\\') + file_prefix + k + '.csv', 'wb') as out_file:
                writer = csv.writer(out_file)
                writer.writerows(temp_out)
            sql_copy = """COPY temp_{0} from 'E:\\Data\\Census\\DEC\\2010\\csv\\{1}' DELIMITER ',' CSV;""".format(k, file_prefix + k + '.csv')
            try:
                print '\tInserting data for temp_{0}.{1}'.format(file_prefix, k)
                cursor.execute(sql_copy)
                db.commit()
            except Exception as e:
                db.rollback()
                print '\n******************Error with table {0}, from file {1}**********************\n'.format(k, vals[1][2]) 
                error = open(os.path.join(census_directory, 'errors.txt'), 'a')
#                     writer = csv.writer(error, delimiter =',')
                m = 'Key: {0}, \tStart: {1}, \tEnd: {2}, \tFile: {3}\tError: {4}'.format(k, vals[1][0] , vals[1][1], vals[1][2], e.message)
                error.write(m)
                error.close()
    build_geo_header()  
                
def get_directory(directory):
    '''returns a list of all state subfolders 
    
            directory: path to folder containing state subfolders'''
    
    dir_list = []
    for root, dirs, files in os.walk(directory):
        for d in dirs:
            dir_list.append(os.path.join(root,d))
    return dir_list

def accdb_connect(census_directory):
    '''Connects to Access Database holding all of the table shells and schema inforomation    
        census_directory: root path where all data and tables are stored'''
    import pyodbc
    cnx_accdb = "Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=" + census_directory + '/SF1_Access2007.accdb'
    conn_accdb = pyodbc.connect(cnx_accdb)
    return conn_accdb.cursor()
    
def get_schema_list():
    accdb_cursor = accdb_connect(census_directory)
    accdb_cursor.execute("select * from DATA_FIELD_DESCRIPTORS")
    rows = accdb_cursor.fetchall()
    return rows

def build_geo_header():
    print 'Building geo_header table... '
    accdb_cursor = accdb_connect(census_directory)
    field_mask = ''
    geo_header = {}
    '''open GEO_HEADER_SF1 table to grab attribute names and simplified data types '''
    for column in accdb_cursor.columns(table = 'GEO_HEADER_SF1'):
        geo_header[column.column_name] = [column.type_name if column.type_name == 'VARCHAR' 
                                          else 'NUMERIC', column.ordinal_position]
    
    '''the field width and start index are populated from the MSysIMEXColumns table from the
    SF1_Access2007.accdb '''
    accdb_cursor.execute("select * from MSysIMEXColumns where SpecID = 4")
    geo_header_sort = {}
    for row in accdb_cursor.fetchall():
        geo_header_sort[geo_header[row[2]][1]] = [row[2],geo_header[row[2]][0], row[6], row[7]]        
     
    '''convert geo_header_sort keys into sorted list by field index number to guarantee correct order
    when table is created '''
    sorted_list = sorted(geo_header_sort.keys())
    sql_stmnt = []
    for i in sorted_list:    
        field_mask +=  str(geo_header_sort[i][3]) + 's'
        '''if column.type_name is varchar, the length of the field must be appended, 
        otherwise the data type is just numeric'''
        type = ['(' + str(geo_header_sort[i][3]) + ')' if geo_header_sort[i][1] == 'VARCHAR' else '']
        sql_stmnt.append(geo_header_sort[i][0] + ' ' + geo_header_sort[i][1] + type[0])        
    sql_create = """CREATE TABLE GEO_HEADER_SF1 ({0})"""
    cursor.execute(sql_create.format(', '.join(f for f in sql_stmnt)))
    db.commit()
    load_geo_header(field_mask)

def load_geo_header(field_mask):
    import struct    
    directory = get_directory(os.path.join(census_directory, 'sf1', ))
    for state_folder in directory:
        print 'Loading geo_header data from ', state_folder
        with open(os.path.join(state_folder, '{0}geo2010.sf1'.format(state_folder.split('\\')[-1]))) as f:
            for line in f:
                row = struct.Struct(field_mask).unpack_from(line.replace("'", " "))
                fields = tuple(field.strip() for field in row)
                sql_insert = """INSERT INTO geo_header_sf1 values {0};""".format(fields)
                cursor.execute(sql_insert)
                db.commit()
    
def build_final_table(tbl_schema):
    sql_select = """create table {0} as(
    SELECT DISTINCT name, {1}.stusab, {1}.logrecno, sumlev,CASE 
    when sumlev = '040' then STATE
    when sumlev = '050' then concat(STATE, COUNTY)
    when sumlev = '060' then concat(STATE, COUNTY, COUSUB)
    when sumlev = '070' then concat(STATE, COUNTY, COUSUB,PLACE)
    when sumlev = '101' then concat(STATE, COUNTY, TRACT, BLOCK)    
    when sumlev = '140' then concat(STATE, COUNTY, TRACT)
    when sumlev = '150' then concat(STATE, COUNTY, TRACT, BLKGRP)    
    when sumlev = '160' then concat(STATE, PLACE)
    when sumlev = '170' then concat(STATE, CONCIT)    
    when sumlev = '320' then CBSA
    when sumlev = '340' then concat(STATE, CSA)  
    when sumlev = '500' then concat(STATE, CD)
    when sumlev = '610' then concat(STATE, SLDU, SUMLEV)
    when sumlev = '620' then concat(STATE, SLDL, SUMLEV)
    when sumlev = '860' then concat(STATE,ZCTA5)    
    when sumlev = '950' then concat(STATE, SDELEM)
    when sumlev = '960' then concat(STATE, SDSEC)
    when sumlev = '970' then concat(STATE, SDUNI) 
    end as geoid,
    {2}
    FROM {1} join {3} on 
    (({3}.stusab = {1}.stusab) and
    ({3}.logrecno = {1}.logrecno)) and sumlev in ('040', '050', '060', '070', '101', '140', '150', '160', '170', 
    '320', '340', '500', '610', '620','860', '950', '960', '970'))"""
    count = 1
    for k, vals in tbl_schema.items():                
        fields = ', '.join(v[0] for v in vals[2])
        print '\tFinalizing table {0} of {1}'.format(count, len(tbl_schema.keys()))
        count += 1
        cursor.execute(sql_select.format(k, 'temp_' + k, fields, 'geo_header_sf1'))
        db.commit()
    schema_to_comment(tbl_schema)
          
def main():    
    reader = get_schema_list()
    
    tbl = [r for r in reader]
    row = 0
    current_key = ''
    tbl_start_index = 6
    tbl_finish_index = 0
    tbl_indices = [tbl_start_index, tbl_finish_index, tbl[row][1].zfill(5)]
    file_length = 0
    while row < len(tbl):
        '''Creates new table and adds table name to dictionary keys if it doesn't exist'''            
        if key_exists(tbl[row][2]) == False:
            if len(tbl_schema.keys()) > 0:
                '''Each row  '''
                tbl_len =  len(tbl_schema[tbl[row-1][2]][2])            
                tbl_finish_index = (tbl_len + tbl_start_index) - 1
                tbl_schema[tbl[row-1][2]][1][0] = tbl_start_index 
                tbl_schema[tbl[row-1][2]][1][1] = tbl_finish_index
                tbl_schema[tbl[row-1][2]][1][2] = tbl[row-1][1].zfill(5)
                tbl_indices = [tbl_start_index, tbl_finish_index, tbl[row-1][1].zfill(5)]
                file_length += tbl_len
                '''start_index must be incremented after assignment'''
                tbl_start_index = tbl_finish_index
    #                 print '\t', tbl_start_index, tbl_finish_index 
                '''resets start index if current row sequence number is not the same as previous'''
                if tbl[row-1][1] != tbl[row][1]:
                    tbl_start_index = 6
                    tbl_finish_index = 0
            current_key = tbl[row][2]
            field_list = [tbl[row][3].split('[')[0],tbl_indices, []]
            add_key(tbl[row][2])
            row += 1         
            ''''handles creation of field names and comments'''
        elif tbl[row][4] != None:
            field = tbl[row][4].lower()
            comment = tbl[row][3].strip().replace("'", "''")
            field_list[2].append((field, comment))
            tbl_schema[current_key] = field_list 
            row += 1             
        else:
            row += 1
    for k in tbl_schema.keys():
        print k, tbl_schema[k]
    #insert_data(tbl_schema)
    #build_temp_tables(tbl_schema)
    #build_final_table(tbl_schema)
    #build_geo_header()
print main()
# if __name__=='main':
#     main()
        