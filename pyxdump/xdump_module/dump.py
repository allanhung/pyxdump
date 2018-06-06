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
import common

def schema(args):
    exclude_list = ['mysql' ,'information_schema', 'performance_schema', 'sys']
    if args['--exclude_database']:
        exclude_list.extend(args['--exclude_database'])
    exclude_str = ''
    if exclude_list: 
        exclude_str = "'"+"','".join(exclude_list)+"'"
    connect_list = []
    if args['--user']:
        connect_list.append('-u{0}'.format(args['--user']))
    if args['--password']:
        connect_list.append('-p{0}'.format(args['--password']))
    if args['--database']:
        db_list = args['--database'].split(',')
    else:
        sqlscript = 'SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('+exclude_str+')'
        db_list = (common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
    sqlscript = "show global variables like 'innodb_default_row_format'"
    rlist = common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True).strip().split('\n')
    if rlist[0]:
        row_format = rlist[0].split('\t')[1]
    else:
        row_format = 'COMPACT'
    if args['--db_per_file'] or len(db_list) == 1:
        for db in db_list:
            subprocess.call('mysqldump {0} --no-data --set-gtid-purged=OFF --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {1} --result-file={2}.sql'.format(' '.join(connect_list),db,os.path.join(args['--output_dir'],db)), shell=True)
            subprocess.call('sed -i -e "s/ENGINE=InnoDB/ENGINE=InnoDB ROW_FORMAT={0}/g" {1}.sql'.format(row_format,os.path.join(args['--output_dir'],db)),shell=True)
            print('Output File: {0}.sql'.format(os.path.join(args['--output_dir'],db)))
            if args['--create_user']:
                subprocess.call('pt-show-grants {0} >> {1}.sql'.format(' '.join(connect_list),os.path.join(args['--output_dir'],db)), shell=True)
    else:
        subprocess.call('mysqldump {0} --no-data --set-gtid-purged=OFF --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {1} --result-file={2}.sql'.format(' '.join(connect_list),' '.join(db_list),os.path.join(args['--output_dir'],'alldb')), shell=True)
        subprocess.call('sed -i -e "s/ENGINE=InnoDB/ENGINE=InnoDB ROW_FORMAT={0}/g" {1}.sql'.format(row_format,os.path.join(args['--output_dir'],'alldb')),shell=True)
        if args['--create_user']:
            subprocess.call('pt-show-grants {0} >> {1}.sql'.format(' '.join(connect_list),os.path.join(args['--output_dir'],'alldb')), shell=True)
        print('Output File: {0}.sql'.format(os.path.join(args['--output_dir'],'alldb')))

    return None
