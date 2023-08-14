# lustre-scripts
Useful scripts based on the Lustre file system (http://lustre.org/) and the Robinhood Policy Engine (https://github.com/cea-hpc/robinhood).

## Lustre Job Analyser

__Description:__

Produce an overview of the Slurm jobs that have a higher write or read sample count based on the Lustre job statistics regarding specfied file server (OSS).

__Requisites:__

* Enabled Lustre job stats.
* _ClusterShell_ - For executing remote commands in parallel from a workstation on the cluster.
* _show_high_jobstats.pl_ - Perl script to collect and filter queried jobstats from Lustre OSS
(https://www.scc.kit.edu/scc/sw/lustre_tools/show_high_jobstats.tgz).
* _slurm-client_ - Squeue command to query Slurm information about jobs.

__Script Parameter:__

* -D/--enable-debug: Enables debug log messages.
* -u/--user: User for executing remote commands e.g. querying Lustre job statitics or squeue information from Slurm.
* -s/--oss-nodes: Specification of OSS nodes by using ClusterShell NodeSet syntax.
* -n/--client-node: Specification of Client Node.
* -m/--min-samples: Minimum number of read or write Lustre jobstats sample count.

__Script Execution:__

```
lustre_job_analyser.py -s file_server[000-100] -n client_node0200 -u admin_user -m 10000
```

__Output Schema:__

SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST
...
SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST

## Lustre Collect Changelog Indexes

__Description:__

This script collects changelog indexes from a Lustre MDT.
It can be executed continuously or just in a capture mode.

for displaying the delta between the Lustre changelog index and the specified changelog reader index.

__Script Parameter:__

* -i/--interval: Specifies the collect interval in seconds.
* -d/--delimiter: Defines the delimiter for unloaded indexes.
* -m/--mdt-name: Specifies the MDT name where to read the current index from.
* -r/--changelog-reader: Specifies an additional changelog reader to read the index from.
* -f/--unload-file: Specifies the unload file where the collected information is saved (changelog_indexes.unl).
* --direct-flush: If enabled after each collection interval a disk write flush is done of the collected values.
* --capture-delta: Prints the delta between the changelog consumer and the MDT index after one interval on the stdout and quits.

__Script Execution:__

Continuous Collect Mode:
```
lustre_collect_changelog_indexes.py -m fs-MDT0000 -r cl1
```

Capture Delta Mode:
```
lustre_collect_changelog_indexes.py -m fs-MDT0000 -r cl1 --capture-delta
```

__Output Schema:__

In Continuous Collect Mode:
* TIMESTAMP;
* MDT CHANGELOG INDEX PRODUCER COUNT;
* CHANGELOG READER INDEX CONSUMER COUNT;
* DELTA BETWEEN CHANGELOG READER INDEX AND MDT CHANGELOG INDEX

In Capture Delta Mode:
DELTA BETWEEN CHANGELOG READER INDEX AND MDT CHANGELOG INDEX

## Lustre Large File Notifier

__Description:__

This program queries the Robinhood Entries database table for large files and keeps track of that files in separate notification table. Based on the notification table user can be notified of large files saved on the Lustre file system and/or an overview of large files can be send to administrators of the file system.

__Requisites:__

python-mysqldb - Python interface to MySQL
LDAP service running for determining users email adress on user notification.

__Script Parameter:__

* -f/--config-file: Path of the config file.
* -D/-enable-debug: Enables logging of debug messages.
* --create-table: If set the notifiers table is created.
* --no-mail: Disables mail send.

__Structure of the Configuration File:__

```
[mysqld]
host         =
database     =
user         =
password     =

[check]
file_size            = 500GB
file_system          =
check_interval_days  = 7

[notify]
table    = NOTIFIES
database =

[mail]
server             =
sender             =
overview_recipient =
subject            = Large File Report
send_user_mail     = off

[ldap]
server =
dc     =
```

__Script Execution:__

Executing the rbh-large-file-notifier with debug messages saved into a proper log file:

```
./rbh-large-file-notifier.py -f rbh-large-file-notifier.conf -D >> rbh-large-file-notifier.log 2>&1
```

__Schema of the Notifier Table:__

```
+---------------+----------------------+------+-----+---------+-------+
| Field         | Type                 | Null | Key | Default | Extra |
+---------------+----------------------+------+-----+---------+-------+
| fid           | varbinary(64)        | NO   | PRI | NULL    |       |
| uid           | varbinary(127)       | NO   |     | NULL    |       |
| size          | bigint(20)           | NO   |     | NULL    |       |
| path          | varchar(1000)        | NO   |     | NULL    |       |
| last_check    | datetime             | NO   |     | NULL    |       |
| last_notify   | datetime             | YES  |     | NULL    |       |
| ignore_notify | enum('FALSE','TRUE') | YES  |     | FALSE   |       |
+---------------+----------------------+------+-----+---------+-------+
7 rows in set (0.00 sec)

```

__Example Mail Text for Administrators:__

```
Dear All,

this is the automated report of stored large files on '/lustre/fs' that are equal or larger than 500GB.

The following information is provided in CSV format: uid;size;path;last_notify

...
```

__Example Mail Text for Users:__

```
Dear user *uid*,

this is an automated e-mail that contains a list of your stored files on '/lustre/fs' that are equal or larger than 500GB.

Please check if you really need those files stored on the file system.

...
```

## Robinhood Unload Processing Tool

Script: `rbh-ost-file-map-creator.py`

__Description:__

Creates mapping files between start OST index and filepath based on Robinhood unloads generated with rbh-report.

This tool is used to create input files for file migration on Lustre with the [Cyclone Framework](https://github.com/GSI-HPC/cyclone-distributed-task-driven-framework).

__Script Parameter:__

```
options:
  -h, --help            show this help message and exit
  -e FILENAME_EXT, --filename-ext FILENAME_EXT
                        Default: .unl
  -f FILENAME_PATTERN, --filename-pattern FILENAME_PATTERN
                        For instance: file_class_ost{INDEX}, where {INDEX} is a placeholder for the OST index.
  -i OST_INDEXES, --ost-indexes OST_INDEXES
                        Defines a RangeSet for the OST indexes e.g. 0-30,75,87-103
  -s SPLIT_INDEX, --split-index SPLIT_INDEX
                        Default: 1
  -w WORK_DIR, --work-dir WORK_DIR
                        Default: '.'
  -x EXACT_FILENAME, --exact-filename EXACT_FILENAME
                        Explicit filename to process.
  -l LOG_FILE, --log-file LOG_FILE
                        Specifies logging file.
  -D, --enable-debug    Enables logging of debug messages.
```

__Usage of rbh-report:__

The following creates the unload files with `rbh-report`:

```
FILE_CLASS=XXX

for i in {280..310}
do
    rbh-report --dump-ost ${i} --filter-class=${FILE_CLASS} --csv > ${FILE_CLASS}_ost${i}.unl &
done
```

The unload files must be created in CSV format and with header.
