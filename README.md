# pyxdump

MySQL export tool

## How to Build
```sh
$ pip install -r https://raw.githubusercontent.com/allanhung/pyxdump/master/requirements.txt
$ pip install git+https://github.com/allanhung/pyxdump
```
    
## Example
### export
```sh
# export table create script and create user script
$ pyxdump export schema --create_user
# export table 
$ pyxdump export table --table_list <schema1.table1,schema2.table2> --output_dir <output_dir> --remote <user@ip> --remote_cmd 'sshpass -e <cmd>'
# export from innobackup
$ innobackupex --no-lock --no-timestamp <backup_dir>
# send to remote
$ tar jcf - <backup_dir> | ssh user@ip "cat > <remotr_dir>/`date +%Y%m%d`.tar.bz2"
# prepear to export 
$ innobackupex --use-memory=<memory_size> --apply-log --export <backup_dir>>
```

### import
```sh
# import schema
$ pyxdump import schema --script_file <script_file>
# import data from backup
$ pyxdump import data --backupdir <backup_dir> --datadir <mysql_data_dir>
# import data by table
$ pyxdump import data --backupdir <backup_dir> --datadir <mysql_data_dir>  --table_list <schema1.table1,schema2.table2>
```
