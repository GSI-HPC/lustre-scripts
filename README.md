# lustre-scripts
Useful scripts for the Lustre file system (http://lustre.org/) and the Robinhood Policy Engine (https://github.com/cea-hpc/robinhood).

## Lustre Job Analyser

__Description:__

Produce an overview of the SLurm jobs that have a higher write or read sample count based on the Lustre job statistics regarding specfied file server.

__Requisites:__

* Cluster Shell (Clush) - local dependency
* Slurm Queue command on remote client node
* Enabled Lustre job statistics on the file server

__Script Parameter:__

* -D: Enable Debug Messages
* -u: User for executing remote commands e.g. querying Lustre job statitics or squeue information from Slurm.

__Script Execution:__

```
lustre_job_analyser.py -s file_server[000-100] -n client_node0200 -u admin_user -m 1000 -j lustre_jobstats_file.unl
```

__Output Schema:__

SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST  
...  
SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST  
