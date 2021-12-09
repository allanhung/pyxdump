#!/usr/bin/python

"""
mysql compare record count

Usage:
  pyxdump compare data  --srcUser srcUser --srcPassword srcPassword --srcHost srcHost --dstUser dstUser --dstPassword dstPassword --dstHost dstHost [--exclude_database EXDB]

Arguments:
  --srcUser srcUser         source database login user
  --srcPassword srcPassword source database login password
  --srcHost srcHost         source database host
  --dstUser srcUser         destination database login user
  --dstPassword dstPassword destination database login password
  --dstHost dstHost         destination database host
Options:
  --exclude_database EXDB   database exclude list (example: db3,db4)
  -h --help                 Show this screen.
"""

from docopt import docopt
import pymysql
from colorclass import Color
from terminaltables import SingleTable
from tqdm import tqdm

def getRecordCount(db_connect, schema_name, table_name):
    sqlscript = 'select count(*) from {0}.{1}'.format(schema_name, table_name) 
    try:
        db_connect.execute(sqlscript)
        data = db_connect.fetchone()
        return data[0]
    except Exception as e:
        return str(e)

def data(args):
    exclude_list = ['information_schema', 'innodb', 'mysql', 'performance_schema', 'sys', 'tmp']
    if args['--exclude_database']:
        exclude_list.extend(args['--exclude_database'].split(","))
    exclude_str = ''
    if exclude_list: 
        exclude_str = "'"+"','".join(exclude_list)+"'"
    db_list = []
    tb_list = []
    srcDb = pymysql.connect(host=args['--srcHost'], user=args['--srcUser'], password=args['--srcPassword'])
    dstDb = pymysql.connect(host=args['--dstHost'], user=args['--dstUser'], password=args['--dstPassword'])
    srcCursor = srcDb.cursor()
    dstCursor = dstDb.cursor()
    
    sqlscript = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('{0}')".format("','".join(exclude_list))
    srcCursor.execute(sqlscript)
    srcData = srcCursor.fetchall()
    for row in srcData:
        db_list.append(row[0])
    sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' and ENGINE = 'InnoDB' and table_schema in ('{0}')".format("','".join(db_list))
    srcCursor.execute(sqlscript)
    srcData = srcCursor.fetchall()
    table_data = [['table name', args['--srcHost'], args['--dstHost']]]
    for row in tqdm(srcData):
        srcCount = getRecordCount(srcCursor, row[0], row[1])
        dstCount = getRecordCount(dstCursor, row[0], row[1])
        if srcCount != dstCount:
            table_data.append([Color('{autored}%s{/autored}' % (row[0]+'.'+row[1])), Color('{autored}%s{/autored}' % srcCount), Color('{autored}%s{/autored}' % dstCount)])
        else:
            table_data.append([row[0]+'.'+row[1], srcCount, dstCount])
    srcDb.close()
    dstDb.close()
    print(SingleTable(table_data).table)
    return None
