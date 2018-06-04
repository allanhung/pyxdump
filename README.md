# pyxdump

MySQL schema dump tool

## How to Build
```sh
$ pip install git+https://github.com/allan-hung/pyxdump
```
    
## Example
### dump
```sh
$ pyxdump dump schema --user root --password dbpass
```

### import
```sh
$ pyxdump import schema --script_file /tmp/alldb.sql
$ pyxdump import data --backupdir /dbbackup/20180604 --datadir /data/mysql
```
