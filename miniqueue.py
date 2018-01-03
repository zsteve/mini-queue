#!/usr/bin/python

import sys
import os
import time

''' 
job_dist2: simple queueing of small jobs on a large number of computer systems

job_dist2 [job_list] [machine_list]

input files:
[job_list] - a list of all input jobs to be run on systems. One line of bash command(s) per job

[machine_list] - a list of all machines that jobs can be run on.
Format:
[machine address] [max number of jobs at any time]
'''

class Machine:

    def __init__(self, addr, n_max_jobs):
        self.addr = addr
        self.n_max_jobs = n_max_jobs
        self.pid_list = []
        return

    def has_pid(self, pid):
        return pid in self.pid_list

    def add_pid(self, pid):
        self.pid_list+=pid
        return

    def run_job(self, job_strings):
        job_str = "ssh %s '" % self.addr
        for i in job_strings:
            # concatenate all our job strings
            job_str+=("%s > /dev/null& echo $!;" % i)
        
        job_str+="'"
        pid=os.popen(job_str).read() # submit the job but don't block  
        print pid
        for i in pid.splitlines():
            if(i.isdigit()):
                self.pid_list+=[int(i)]
        return

    def get_procnum(self):
        # retrieve number of running jobs on this machine
        ps_output = os.popen("ssh %s 'ps -e' | awk '{print $1}'" % self.addr).read()
        n_jobs = 0
        #print ps_output.splitlines()
        for pid in ps_output.splitlines():
            if pid.isdigit() and int(pid) in self.pid_list:
                # int(pid) is the process id
                n_jobs+=1
        return n_jobs

    def num_completed(self):
        # retrieve the number of completed jobs from this machine
        return len(self.pid_list) - self.get_procnum()

job_list_file = sys.argv[-2]
machine_list_file = sys.argv[-1]

job_list = []
machine_list = []

with open(job_list_file) as f:
    for i in f:
        # get commands 
        job_list+=[i.strip('\n')]

with open(machine_list_file) as f:
    for i in f:
        # get machines
        (addr, n_job_max) = i.split()
        machine_list+=[Machine(addr, int(n_job_max))]

print job_list
print machine_list

num_jobs_submitted = 0

while True:
    for m in machine_list:
        # loop through each machine and get the number of processes
        num_new_jobs = m.n_max_jobs - m.get_procnum()
        sub_jobs = []
        if num_new_jobs > 0 and len(job_list) >= num_new_jobs:
            # pop num_new_jobs jobs and send these all to the machine
            for i in range(0, num_new_jobs):
                sub_jobs+=[job_list.pop()]
            m.run_job(sub_jobs)
            num_jobs_submitted+=num_new_jobs
        elif num_new_jobs > 0 and len(job_list) < num_new_jobs:
            # submit _all_ remaining jobs
            num_new_jobs = len(job_list)
            sub_jobs = job_list
            m.run_job(sub_jobs)
            num_jobs_submitted+=num_new_jobs
            job_list = []
        elif num_new_jobs <= 0:
            # machine is full so do nothing
            # nothing of interest here...
            pass

        if num_new_jobs > 0:
            print 'Submitted %d new jobs to %s. Machine has current load %d, and completed %d jobs | Total jobs submitted: %d' % \
                (num_new_jobs, m.addr, m.get_procnum(), m.num_completed(), num_jobs_submitted)
            
            #print m.pid_list

        if len(job_list) == 0:
            break
    
    if len(job_list) == 0:
        # no more new jobs, so just wait for all existing jobs to finish
        print "Out of jobs! Waiting for all existing jobs to terminate..."
        for m in machine_list:
            while m.get_procnum() > 0:
                time.sleep(1.0)
        break
    # now sleep :)
    time.sleep(1.0)
