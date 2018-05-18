# lustre-scripts
Useful scripts for the Lustre file system (http://lustre.org/) and the Robinhood Policy Engine (https://github.com/cea-hpc/robinhood).

## Script Collection

# Lustre Job Analyser

```
lustre_job_analyser.py -s file_server[000-100] -n client_node0200 -u admin_user -m 1000 -j lustre_jobstats_file.unl
```
__Output:__

SLURM_JOB_ID|USER|GROUP|EXECUTABLE|CLIENT_NODE_LIST|FILE_SERVER_LIST
