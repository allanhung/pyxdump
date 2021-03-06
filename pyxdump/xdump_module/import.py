#!/usr/bin/python

"""
mysql import schema and data

Usage:
  pyxdump import schema [--user USER] [--password PASSWORD] [--script_file SCRIPTFILE]
  pyxdump import data --backupdir BACKUPDIR --datadir DATADIR [--table_list TBLIST] [--user USER] [--password PASSWORD] [--mysql_os_user OSUSER] [--mysql_os_group OSGROUP]  [--database DATABASE] [--exclude_database EXDB] [--pxc]
  pyxdump import fix --backupdir BACKUPDIR --table_list TBLIST [--fix_dir FDIR] [--fix_host FHOST] [--mysql_src_ver MSV] [--mysql_dst_ver MDV] [--sshuser SSHUSER]

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
  --table_list TBLIST       table list (example: db1.tb1,db1.tb2)
  --mysql_src_ver MSV       MySQL Version for export host [default: 5.6]
  --mysql_dst_ver MSV       MySQL Version for target host [default: 5.7]
  --sshuser SSHUSER         MySQL Version for target host [default: root]
  --fix_host FHOST          Machine can docker (example: 192.168.1.1) [default: localhost]
  --fix_dir FDIR            Target dir (example: /tmp) [default: /tmp]
  --exclude_database EXDB   database exclude list (example: db3,db4)
  --pxc                     If is Percona Xtra Cluster
  -h --help                 Show this screen.
"""

from docopt import docopt
import subprocess
import os
from . import common

def fix(args):
    tb_list=args['--table_list'].replace('.','\t').split(',')
    print(fix_import(args, tb_list, args['--mysql_src_ver'], args['--mysql_dst_ver'], args['--fix_host']))
    return None

def fix_import(args, tb_list, mysql_src_version, mysql_dst_version, fix_host):
    tmp_dir=args['--fix_dir']
    fix_base_dir=os.path.join(tmp_dir,'fix')
    tmp_dbpass='dbpass'
    connect_list=[]
    connect_list.append('-uroot')
    connect_list.append('-p{0}'.format(tmp_dbpass))
    fix_script=[]
    if fix_host != 'localhost':
        fix_script.append('# on source')
        for tb in tb_list:
            (schema, table) = tb.split('\t')
            fix_script.append('sshpass -e ssh {0}@{1} mkdir -p {2}'.format(args['--sshuser'], fix_host, os.path.join(tmp_dir,schema)))
            fix_script.append('sshpass -e scp {0}.{{cfg,ibd}} {1}@{2}:{3}/'.format(os.path.join(args['--backupdir'],schema,table), args['--sshuser'], fix_host, os.path.join(tmp_dir,schema)))
            fix_script.append('sshpass -e scp /tmp/{0}.{1}.sql {2}@{3}:{4}/{0}/{1}.sql'.format(schema, table, args['--sshuser'], fix_host, os.path.join(tmp_dir)))

    if fix_host != 'localhost':
        fix_script.append('# on {0}'.format(fix_host))
    fix_script.append('docker run -d --name=fix_mysql -e MYSQL_ROOT_PASSWORD={0} -v {1}/data:/var/lib/mysql -v {1}/backup:/dbbackup -v {1}/export:/export mysql:{2}'.format(tmp_dbpass, fix_base_dir, mysql_src_version))
    for tb in tb_list:
        (schema, table) = tb.split('\t')
        fix_script.append('mkdir -p {0}'.format(os.path.join(fix_base_dir,'backup',schema)))
        if fix_host == 'localhost':
            fix_script.append('/bin/cp -f {0}.{{cfg,ibd}} {1}'.format(os.path.join(args['--backupdir'],schema,table), os.path.join(fix_base_dir,'backup',schema)))
            fix_script.append('/bin/cp -f /tmp/{0}.{1}.sql {2}/{1}.sql'.format(schema, table, os.path.join(fix_base_dir,'backup',schema)))
        else:
            fix_script.append('mv {0}.{{cfg,ibd,sql}} {1}'.format(os.path.join(tmp_dir,schema,table), os.path.join(fix_base_dir,'backup',schema)))
        sqlscript="create database if not exists {0};".format(schema)
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))
        sqlscript="use {0}; source {1}/{2}.sql;".format(schema, os.path.join('/dbbackup',schema), table)
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))
        sqlscript="alter table {0}.\`{1}\` discard tablespace;".format(schema, table)
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))
        fix_script.append('mv {0}/{1}/{2}.{{cfg,ibd}} {3}/{1}/'.format(os.path.join(fix_base_dir,'backup'), schema, table, os.path.join(fix_base_dir,'data')))
        fix_script.append('docker exec fix_mysql chown mysql.mysql -R /var/lib/mysql/{0}'.format(schema))
        sqlscript="alter table {0}.\`{1}\` import tablespace;".format(schema, table)
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))

    fix_script.append('docker stop fix_mysql && docker rm fix_mysql')
    fix_script.append('docker run -d --name=fix_mysql -e MYSQL_ROOT_PASSWORD={0} -v {1}/data:/var/lib/mysql -v {1}/backup:/dbbackup -v {1}/export:/export mysql:{2}'.format(tmp_dbpass, fix_base_dir, mysql_dst_version))
    fix_script.append('docker exec fix_mysql mysql_upgrade -uroot -p{}'.format(tmp_dbpass))
    for tb in tb_list:
        (schema, table) = tb.split('\t')
        fix_script.append('docker exec fix_mysql mkdir -p /export/{0}'.format(schema))
        sqlscript="use {0}; flush tables \`{1}\` for export;\!bash cp /var/lib/mysql/{0}/{1}.cfg /export/{0}/ && cp /var/lib/mysql/{0}/{1}.ibd /export/{0}/".format(schema, table)
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))
        sqlscript="unlock tables;"
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))
        sqlscript="use {0}; drop  table \`{1}\`;".format(schema, table)
        fix_script.append('docker exec fix_mysql mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript))
    fix_script.append('docker stop fix_mysql && docker rm fix_mysql')

    if fix_host != 'localhost':
        fix_script.append('# on source')
    for tb in tb_list:
        (schema, table) = tb.split('\t')
        fix_script.append('mv {0}.cfg {0}.cfg.bak'.format(os.path.join(args['--backupdir'],schema,table)))
        fix_script.append('mv {0}.ibd {0}.ibd.bak'.format(os.path.join(args['--backupdir'],schema,table)))
        if fix_host == 'localhost':
            fix_script.append('cp {0}.cfg {1}'.format(os.path.join(fix_base_dir,'export',schema,table),os.path.join(args['--backupdir'],schema)))
            fix_script.append('cp {0}.ibd {1}'.format(os.path.join(fix_base_dir,'export',schema,table),os.path.join(args['--backupdir'],schema)))
        else:
            fix_script.append('sshpass -e scp {0}@{1}:{2}.cfg {3}'.format(args['--sshuser'], fix_host, os.path.join(fix_base_dir,'export',schema,table),os.path.join(args['--backupdir'],schema)))
            fix_script.append('sshpass -e scp {0}@{1}:{2}.ibd {3}'.format(args['--sshuser'], fix_host, os.path.join(fix_base_dir,'export',schema,table),os.path.join(args['--backupdir'],schema)))
    fix_script.append('run follow command to import data:\npyxdump import data --backupdir {0} --table_list {1} --datadir --user --password'.format(args['--backupdir'],args['--table_list']))
    return '\n'.join(fix_script)

def schema(args):
    connect_list = []
    if args['--user']:
        connect_list.append('-u{0}'.format(args['--user']))
    if args['--password']:
        connect_list.append('-p\'{0}\''.format(args['--password']))
    rc = subprocess.call('mysql {0} < {1}'.format(' '.join(connect_list),args['--script_file']),shell=True)
    sqlscript="select plugin_name, plugin_status, plugin_library from information_schema.plugins where plugin_name = 'validate_password';"
    vp_plugin=common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True).strip().split('\n')
    plugin_context = []
    if vp_plugin[0]:
        plugin_context = vp_plugin[0].split('\t')
        if plugin_context[1] == 'ACTIVE':
            sqlscript="UNINSTALL PLUGIN {0}".format(plugin_context[0])
            subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
    rc = subprocess.call('mysql {0} < {1}'.format(' '.join(connect_list),args['--script_file']),shell=True)
    if plugin_context and plugin_context[1] == 'ACTIVE':
        sqlscript="INSTALL PLUGIN {0} SONAME '{1}'".format(plugin_context[0], plugin_context[2])
        subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
    print('import {0} complete!'.format(args['--script_file']))
    return None

def data(args):
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

    if not tables:
        if args['--database']:
            db_list = args['--database'].split(',')
        else:
            sqlscript = 'SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('+exclude_str+')'
            db_list = (common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
        sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' and ENGINE = 'InnoDB' and table_schema in ('{0}')".format("','".join(db_list))
        tables = (common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')

    session_variable = 'SET global pxc_strict_mode=DISABLED;' if args['--pxc'] else ''
    lost_bakfile_list = []
    import_failed_list = []
    import_failed_xlist = []
    DEVNULL = open(os.devnull, 'w')
    sqlscript="SET GLOBAL FOREIGN_KEY_CHECKS=0;"
    subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
    for tb in tables:
        (schema, table) = tb.split('\t')
        p = subprocess.Popen('ls -l {0}.{{cfg,ibd}}'.format(os.path.join(args['--backupdir'],schema,table.replace('.','\@002e')),os.path.join(args['--datadir'],schema)),shell=True, stdout=DEVNULL, stderr=DEVNULL)
        p.wait()
        if p.returncode == 0:
            sqlscript="SET SESSION sql_log_bin=0; {0} truncate table {1}.\`{2}\`; alter table {1}.\`{2}\` discard tablespace;".format(session_variable, schema, table)
            print('running sql script: {0}'.format(sqlscript))
            subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
            print('copy table data: {0}.{1}'.format(schema, table))
            subprocess.check_call('/bin/cp -f {0}.{{cfg,ibd}} {1}/'.format(os.path.join(args['--backupdir'],schema,table.replace('.','\@002e')),os.path.join(args['--datadir'],schema)),shell=True)
            print('change file permission: {0}.{1}'.format(schema, table))
            subprocess.check_call('chown {0}.{1} {2}.{{cfg,ibd}}'.format(args['--mysql_os_user'],args['--mysql_os_group'],os.path.join(args['--datadir'],schema,table.replace('.','\@002e'))),shell=True)
            sqlscript="SET SESSION sql_log_bin=0; {0} alter table {1}.\`{2}\` import tablespace;".format(session_variable, schema, table)
            print('running sql script: {0}'.format(sqlscript))
            x = subprocess.Popen('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            x.wait()
            x_stdout, x_stderr = x.communicate()
            if x.returncode > 0:
                import_failed_list.append('{0}.{1} import failed! error:\n{2}'.format(schema, table, x_stderr.strip().replace("mysql: [Warning] Using a password on the command line interface can be insecure.\n","")))
                import_failed_xlist.append('{0}.{1}'.format(schema, table))
                subprocess.call('mkdir -p /tmp/{0}'.format(schema),shell=True)
                subprocess.call('mysqldump {0} --no-data --set-gtid-purged=OFF --force --quote-names --dump-date --opt -d {1} {2} --result-file=/tmp/{1}.{2}.sql'.format(' '.join(connect_list),schema,table),shell=True)
                sqlscript="SET SESSION sql_log_bin=0; drop table {0}.\`{1}\`;".format(schema, table)
                subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
                subprocess.call('/bin/rm -f {0}.{{cfg,ibd}}'.format(os.path.join(args['--datadir'],schema,table)),shell=True)
                sqlscript="SET SESSION sql_log_bin=0; use {0}; source /tmp/{0}.{1}.sql;".format(schema, table)
                subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
            else:
                subprocess.call('/bin/rm -f {0}.{{cfg,exp}}'.format(os.path.join(args['--datadir'],schema,table)),shell=True)
        else:
            lost_bakfile_list.append('{0}.{{cfg,ibd}}'.format(os.path.join(args['--backupdir'],schema,table)))
    if args['--pxc']:
        sqlscript="SET global pxc_strict_mode=ENFORCING;"
        subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
    sqlscript="SET GLOBAL FOREIGN_KEY_CHECKS=1;"
    subprocess.call('mysql {0} -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)
    if not args['--table_list']:
        sqlscript = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' and ENGINE = 'MyISAM' and table_schema in ('{0}')".format("','".join(db_list))
        tables = (common.check_output('mysql {0} --batch --skip-column-names -e "{1}"'.format(' '.join(connect_list),sqlscript),shell=True)).strip().split('\n')
        for tb in tables:
            if tb:
                (schema, table) = tb.split('\t')
                print('The engine of table {0}.{1} is MyISAM'.format(schema, table))
    if import_failed_list:
        print('## import failed ####################')
        print('\n'.join(import_failed_list))
        print('## import failed ####################')
    if lost_bakfile_list:
        print('## bak file not exists ##############')
        print('\n'.join(lost_bakfile_list))
        print('## bak file not exists ##############')
    if import_failed_xlist:
        print('run follow command to fix:\npyxdump import fix --backupdir {0} --table_list {1} --fix_host {2} --mysql_src_ver 5.6 --mysql_dst_ver 5.7'.format(args['--backupdir'],','.join(import_failed_xlist),'192.168.1.1'))
    else:
        print('Complete!')
    return None
