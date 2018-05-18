# lustre-scripts
Useful scripts for the Lustre file system (http://lustre.org/)  
and the Robinhood Policy Engine (https://github.com/cea-hpc/robinhood).

## Lustre Job Analyser

__Description:__

Produce an overview of the Slurm jobs that have a higher write or read sample count based on the Lustre job statistics regarding specfied file server (OSS).

__Requisites:__

* ClusterShell - For executing remote commands in parallel from a workstation on the cluster.
* Enabled Lustre job stats on the Lustre OSS.
* show_high_jobstats.pl to collect and filter queried jobstats from Lustre OSS (https://www.scc.kit.edu/scc/sw/lustre_tools/show_high_jobstats.tgz).
* Slurm Queue command on remote client node

__Script Parameter:__

* -D/--enable-debug: Enables debug log messages.
* -u/--user: User for executing remote commands e.g. querying Lustre job statitics or squeue information from Slurm.
* -s/--oss-nodes: Specification of OSS nodes by using ClusterShell NodeSet syntax.
* -n/--client-node: Specification of Client Node.
* -m/--min-samples: Minimum number of read or write Lustre jobstats sample count.
* -C/--create-jobstats-file: Specifies if a new Lustre jobstats file should be created.
* -j/--path-jobstats-file: Specifies path to save Lustre jobstats file.

__Script Execution:__

```
lustre_job_analyser.py -s file_server[000-100] -n client_node0200 -u admin_user -m 1000 -j lustre_jobstats_file.unl
```

__Output Schema:__

SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST  
...  
SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST  
