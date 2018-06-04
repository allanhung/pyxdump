#!/usr/bin/python

"""
mysql import schema and data

Usage:
  pyxdump import schema [--user USER] [--password PASSWORD] [--script_file SCRIPTFILE]
  pyxdump import data --backupdir BACKUPDIR --datadir DATADIR [--user USER] [--password PASSWORD] [--mysql_os_user OSUSER] [--mysql_os_group OSGROUP]  [--database DATABASE] [--exclude_database EXDB]

Arguments:
  --backupdir BACKUPDIR     database backup directory [default: /dbbackup]
  --datadir DATADIR         database data directory [default: /data/mysql]
Options:
  --user USER               database login user
  --password PASSWORD       database login password
  --script_file SCRIPTFILE  script file [default: /tmp/alldb.sql]
  --mysql_os_user OSUSER    os user for mysql [default: mysql]
  --mysql_os_group OSGROUP  os group for mysql [default: mysql]
  --database DATABASE       database list default export all database (example: db1,db2)
  --exclude_database EXDB   database exclude list (example: db3,db4)
  -h --help                 Show this screen.
"""

from docopt import docopt
import subprocess

def schema(args):
    connect_list = []
    if args['--user']:
        connect_list.append('-u{}'.format(args['--user']))
    if args['--password']:
        connect_list.append('-p{}'.format(args['--password']))
    rc = subprocess.call('mysql {} < {}'.format(' '.join(connect_list),script_file),shell=True)
    if rc == 0:
        print('Complete!')
    else:
        print('Error with rc code: {}'.format(rc))
    return None

def data(args):
    exclude_list = ['mysql' ,'information_schema', 'performance_schema']
    if args['--exclude_database']:
        exclude_list.extend(args['--exclude_database'])
    exclude_str = ''
    if exclude_list: 
        exclude_str = "'"+"','".join(exclude_list)+"'"
    connect_list = []
    if args['--user']:
        connect_list.append('-u{}'.format(args['--user']))
    if args['--password']:
        connect_list.append('-p{}'.format(args['--password']))

    if args['--database']:
        db_list = args['--database'].split(',')
    else:
        sqlscript = 'SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('+excluse_str+')'
        db_list = (subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')

    sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema in ('{}')".format("','".join(db_list))
    tables = (subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
    for tb in tables:
        (schema, table) = tb.split('\t')
        sqlscript="SET SESSION sql_log_bin=0; truncate table {}.{}; alter table {}.{} discard tablespace;".format(schema, table, schema, table)
        print('running sql script: {}'.format(sqlscript))
        subprocess.call('mysql {} -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)
        print('copy table data: {}.{}'.format(schema, table))
        subprocess.call('/bin/cp -f {}/{}/{}.{cfg.ibd.exp} {}/{}/'.format(args['--backupdir'],schmea,table,args['--datadir'],schema))
        print('change file permission: {}.{}'.format(schema, table))
        subprocess.call('chown {}.{} {}/{}/{}.{cfg.ibd.exp}'.format(args['mysql_os_user'],args['mysql_os_group'],args['--datadir'],schmea,table))
        sqlscript="SET SESSION sql_log_bin=0; alter table {}.{} import tablespace;".format(schema, table)
        print('running sql script: {}'.format(sqlscript))
        subprocess.call('mysql {} -e {}'.format(' '.join(connect_list),sqlscript),shell=True)

    print('Complete!')
    return None
