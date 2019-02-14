#!/usr/bin/python

"""
mysql export table

Usage:
  pyxdump export table  [--user USER] [--password PASSWORD] [--table_list TBLIST] [--database DATABASE] [--exclude_database EXDB] [--output_dir OUTPUTDIR]

Options:
  --user USER               database login user
  --password PASSWORD       database login password
  --table_list TBLIST       table list if empty use database parameter (example: tb1,tb2)
  --database DATABASE       database list default export all database (example: db1,db2)
  --exclude_database EXDB   database exclude list (example: db3,db4)
  --output_dir OUTPUTDIR    script output dir [default: /tmp]
  -h --help                 Show this screen.
"""

from docopt import docopt
import subprocess
import os
import common

def table(args):
    tables=args['--table_list'].replace('.','\t').split(',') if args['--table_list'] else []
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
        connect_list.append('-p\'{0}\''.format(args['--password']))
    tables=args['--table_list'].replace('.','\t').split(',') if args['--table_list'] else []
    if not tables:
        if args['--database']:
            db_list = args['--database'].split(',')
        else:
            sqlscript = 'SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('+exclude_str+')'
            db_list = (common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
        sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' and ENGINE = 'InnoDB' and table_schema in ('{0}')".format("','".join(db_list))
        tables = (common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')

    sqlscript = "show global variables like 'datadir'"
    data_dir = common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True).strip().split('\n')[0].split('\t')[1]

    exp_failed_list=[]
    exp_failed_xlist=[]
    for tb in tables:
        (schema, table) = tb.split('\t')
        subprocess.call('mkdir -p {0}'.format(os.path.join(args['--output_dir'],schema)),shell=True)
        sqlscript="SET SESSION sql_log_bin=0; use {0}; flush tables \`{1}\` for export;\!bash cp {2}/{0}/{1}.cfg {3}/{0}/ && cp {2}/{0}/{1}.ibd {3}/{0}/".format(schema, table, data_dir, args['--output_dir'])
        print('running sql script: {0}'.format(sqlscript))
        x = subprocess.Popen('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        x.wait()
        x_stdout, x_stderr = x.communicate()
        if x.returncode > 0:
            exp_failed_list.append('{0}.{1} export failed! error:\n{2}'.format(schema, table, x_stderr.strip().replace("mysql: [Warning] Using a password on the command line interface can be insecure.\n","")))
            exp_failed_xlist.append('use {0}; flush tables \`{1}\` for export;'.format(schema, table))
        else:
            subprocess.call('/bin/rm -f {0}.{{cfg,exp}}'.format(os.path.join(data_dir,schema,table)),shell=True)
    if exp_failed_list:
        print('## import failed ####################')
        print('\n'.join(exp_failed_list))
        print('## import failed ####################')
    if exp_failed_xlist:
        print('run follow command in mysql to test:')
        print('\n'.join(exp_failed_xlist))
    else:
        print('Complete!')
    return None
