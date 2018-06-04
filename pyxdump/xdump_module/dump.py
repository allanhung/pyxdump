#!/usr/bin/python

"""
mysql dump schema

Usage:
  pyxdump dump schema  [--user USER] [--password PASSWORD] [--database DATABASE] [--exclude_database EXDB] [--output_dir OUTPUTDIR] [--db_per_file]

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

def schema(args):
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
        sqlscript = 'SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('+exclude_str+')'
        db_list = (subprocess.check_output('mysql {} --batch --skip-column-names -e "{}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
    if args['--db_per_file'] or len(db_list) == 1:
        for db in db_list:
            subprocess.call('mysqldump {} --no-data --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {} --result-file={}/{}.sql'.format(' '.join(connect_list),db,args['--output_dir'],db), shell=True)
    else:
        subprocess.call('mysqldump {} --no-data --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {} --result-file={}/{}.sql'.format(' '.join(connect_list),' '.join(db_list),args['--output_dir'],'alldb'), shell=True)

    print('Complete!')
    return None
