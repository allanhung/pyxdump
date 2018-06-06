#!/usr/bin/python

"""
mysql dump schema

Usage:
  pyxdump dump schema  [--user USER] [--password PASSWORD] [--database DATABASE] [--exclude_database EXDB] [--output_dir OUTPUTDIR] [--db_per_file] [--create_user]

Options:
  --user USER               database login user
  --password PASSWORD       database login password
  --database DATABASE       database list default export all database (example: db1,db2)
  --exclude_database EXDB   database exclude list (example: db3,db4)
  --output_dir OUTPUTDIR    script output dir [default: /tmp]
  -h --help                 Show this screen.
"""

from docopt import docopt
import subprocess
import os

def schema(args):
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
    sqlscript = "show global variables like 'innodb_default_row_format'"
    rlist = subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True).strip().split('\n')
    row_format = rlist[0].split('\t')[1]
    if args['--db_per_file'] or len(db_list) == 1:
        for db in db_list:
            subprocess.call('mysqldump {} --no-data --set-gtid-purged=OFF --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {} --result-file={}.sql'.format(' '.join(connect_list),db,os.path.join(args['--output_dir'],db)), shell=True)
            subprocess.call('sed -i -e "s/ENGINE=InnoDB/ENGINE=InnoDB ROW_FORMAT={}/g" {}.sql'.format(row_format,os.path.join(args['--output_dir'],db)),shell=True)
            print('Output File: {}.sql'.format(os.path.join(args['--output_dir'],db)))
            if args['--create_user']:
                subprocess.call('pt-show-grants {} >> '.format(' '.join(connect_list),os.path.join(args['--output_dir'],db)), shell=True)
    else:
        subprocess.call('mysqldump {} --no-data --set-gtid-purged=OFF --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {} --result-file={}.sql'.format(' '.join(connect_list),' '.join(db_list),os.path.join(args['--output_dir'],'alldb')), shell=True)
        subprocess.call('sed -i -e "s/ENGINE=InnoDB/ENGINE=InnoDB ROW_FORMAT={}/g" {}.sql'.format(row_format,os.path.join(args['--output_dir'],'alldb')),shell=True)
        if args['--create_user']:
            subprocess.call('pt-show-grants {} >> '.format(' '.join(connect_list),os.path.join(args['--output_dir'],'alldb')), shell=True)
        print('Output File: {}.sql'.format(os.path.join(args['--output_dir'],'alldb')))

    return None
