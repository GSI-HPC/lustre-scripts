# lustre-scripts
Useful scripts for the Lustre file system (http://lustre.org/)  
and the Robinhood Policy Engine (https://github.com/cea-hpc/robinhood).

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
* -j/--path-jobstats-file: Specifies path to save Lustre jobstats file.
* -C/--create-jobstats-file: Specifies if a new Lustre jobstats file should be created.

__Script Execution:__

```
lustre_job_analyser.py -s file_server[000-100] -n client_node0200 -u admin_user -m 10000 -j lustre_jobstats_file.unl -C
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


## Lustre Storage Report

__Description:__



__Script Parameter:__


__Script Execution:__


__Output Schema:__
