#!/usr/bin/python

"""
mysql export

Usage:
  pyxdump export schema  [--user USER] [--password PASSWORD] [--database DATABASE] [--exclude_database EXDB] [--output_dir OUTPUTDIR] [--db_per_file] [--create_user] [--fix_row_format]
  pyxdump export table  [--user USER] [--password PASSWORD] [--table_list TBLIST] [--database DATABASE] [--exclude_database EXDB] [--output_dir OUTPUTDIR] [--remote REMOTE] [--remote_cmd REMOTECMD]

Options:
  --user USER               database login user
  --password PASSWORD       database login password
  --table_list TBLIST       table list if empty use database parameter (example: tb1,tb2)
  --database DATABASE       database list default export all user database (example: db1,db2)
  --exclude_database EXDB   database exclude list (example: db3,db4)
  --output_dir OUTPUTDIR    script output dir [default: /tmp]
  --db_per_file             generate schema by database
  --create_user             generate create user script
  --fix_row_format          use when import 5.6 to 5.7
  --remote REMOTE           send export file to remote (example: root@10.0.0.1)
  --remote_cmd REMOTECMD    send export file to remote (example: <cmd> -i ~/mykey.pem or sshpass -e <cmd>) [default: sshpass -e <cmd>]
  -h --help                 Show this screen.
"""

from docopt import docopt
import subprocess
import os
from . import common

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
        connect_list.append('-p\'{0}\''.format(args['--password']))
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
            if args['--fix_row_format']:
                subprocess.call('sed -i -e "s/ENGINE=InnoDB/ENGINE=InnoDB ROW_FORMAT={0}/g" {1}.sql'.format(row_format,os.path.join(args['--output_dir'],db)),shell=True)
            print('Output File: {0}.sql'.format(os.path.join(args['--output_dir'],db)))
            if args['--create_user']:
                subprocess.call('pt-show-grants {0} >> {1}.sql'.format(' '.join(connect_list),os.path.join(args['--output_dir'],db)), shell=True)
    else:
        subprocess.call('mysqldump {0} --no-data --set-gtid-purged=OFF --force --quote-names --dump-date --opt --single-transaction --events --routines --triggers --databases {1} --result-file={2}.sql'.format(' '.join(connect_list),' '.join(db_list),os.path.join(args['--output_dir'],'alldb')), shell=True)
        if args['--fix_row_format']:
            subprocess.call('sed -i -e "s/ENGINE=InnoDB/ENGINE=InnoDB ROW_FORMAT={0}/g" {1}.sql'.format(row_format,os.path.join(args['--output_dir'],'alldb')),shell=True)
        if args['--create_user']:
            subprocess.call('pt-show-grants {0} >> {1}.sql'.format(' '.join(connect_list),os.path.join(args['--output_dir'],'alldb')), shell=True)
        print('Output File: {0}.sql'.format(os.path.join(args['--output_dir'],'alldb')))

    return None

def table(args):
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
        if args['--remote']:
            subprocess.call('{0} mkdir -p {1}'.format(args['--remote_cmd'].replace('<cmd>','ssh')+' '+args['--remote'],os.path.join(args['--output_dir'],schema)),shell=True)
            sqlscript="SET SESSION sql_log_bin=0; use {0}; flush tables \`{1}\` for export;\!bash {4} {2}/{0}/{1}.cfg {5}:{3}/{0}/ && {4} {2}/{0}/{1}.ibd {5}:{3}/{0}/".format(schema, table, data_dir, args['--output_dir'], args['--remote_cmd'].replace('<cmd>','scp'), args['--remote'])
        else:
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
