#!/usr/bin/python

"""
mysql import schema and data

Usage:
  pyxdump import schema [--user USER] [--password PASSWORD] [--script_file SCRIPTFILE]
  pyxdump import data --backupdir BACKUPDIR --datadir DATADIR [--user USER] [--password PASSWORD] [--mysql_os_user OSUSER] [--mysql_os_group OSGROUP]  [--database DATABASE] [--exclude_database EXDB] [--pxc]

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
  --pxc                     If is Percona Xtra Cluster
  -h --help                 Show this screen.
"""

from docopt import docopt
import subprocess
import os

def schema(args):
    connect_list = []
    if args['--user']:
        connect_list.append('-u{}'.format(args['--user']))
    if args['--password']:
        connect_list.append('-p{}'.format(args['--password']))
    rc = subprocess.call('mysql {} < {}'.format(' '.join(connect_list),args['--script_file']),shell=True)
    print('import {} complete!'.format(args['--script_file']))
    return None

def data(args):
    exclude_list = ['mysql' ,'information_schema', 'performance_schema', 'sys']
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
        sqlscript = 'SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('+exclude_str+')'
        db_list = (subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')

    session_variable = 'SET global pxc_strict_mode=DISABLED;' if args['--pxc'] else ''
    sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' and ENGINE = 'InnoDB' and table_schema in ('{}')".format("','".join(db_list))
    tables = (subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
    failed_tb_list = []
    DEVNULL = open(os.devnull, 'w')
    for tb in tables:
        (schema, table) = tb.split('\t')
        p = subprocess.Popen('ls -l {}.{{cfg,ibd,exp}}'.format(os.path.join(args['--backupdir'],schema,table),os.path.join(args['--datadir'],schema)),shell=True, stdout=DEVNULL, stderr=DEVNULL)
        p.wait()
        if p.returncode == 0:
            sqlscript="SET SESSION sql_log_bin=0; {} truncate table {}.{}; alter table {}.{} discard tablespace;".format(session_variable, schema, table, schema, table)
            print('running sql script: {}'.format(sqlscript))
            subprocess.call('mysql {} -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)
            print('copy table data: {}.{}'.format(schema, table))
            subprocess.check_call('/bin/cp -f {}.{{cfg,ibd,exp}} {}/'.format(os.path.join(args['--backupdir'],schema,table),os.path.join(args['--datadir'],schema)),shell=True)
            print('change file permission: {}.{}'.format(schema, table))
            subprocess.check_call('chown {}.{} {}.{{cfg,ibd,exp}}'.format(args['--mysql_os_user'],args['--mysql_os_group'],os.path.join(args['--datadir'],schema,table)),shell=True)
            sqlscript="SET SESSION sql_log_bin=0; {} alter table {}.{} import tablespace;".format(session_variable, schema, table)
            print('running sql script: {}'.format(sqlscript))
            subprocess.check_call('mysql {} -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)
        else:
            failed_tb_list.append('{}.{}'.format(schema, table))
    if args['--pxc']:
        sqlscript="SET global pxc_strict_mode=ENFORCING;"
        subprocess.call('mysql {} -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)
    sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' and ENGINE = 'MyISAM' and table_schema in ('{}')".format("','".join(db_list))
    tables = (subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
    for tb in tables:
        (schema, table) = tb.split('\t')
        print('The engine of table {}.{} is MyISAM'.format(schema, table))
    if failed_tb_list:
        print('## failed list ##############')
        print('\n'.join(failed_tb_list))
        print('Complete with failed table!')
    else:
        print('Complete!')
    return None
