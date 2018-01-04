#!/usr/bin/python

import sys
import os
import time
import argparse

'''
Utility for getting machine statistics
usage: stat_get [machinelist] [username]
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
            job_str+=("%s > /dev/null & echo $!;" % i)
        
        job_str+="'"
        pid=os.popen(job_str).read() # submit the job but don't block  
        print pid
        for i in pid.splitlines():
            if(i.isdigit()):
                self.pid_list+=[int(i)]
        return

    def run_cmd(self, cmd):
        return os.popen("ssh %s '%s'" % (self.addr, cmd)).read()

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

print 'statget\nUsage: statget [machinelist] [username]\n'

if len(sys.argv) != 3:
    quit()

machine_list_file = sys.argv[-2]
username = sys.argv[-1]

machine_list = {}

with open(machine_list_file) as f:
    for i in f:
        # get machines
        (addr, n_job_max) = i.split()
        machine_list[addr] = Machine(addr, int(n_job_max))

while True:

    def print_machine_details(m, addr):
            print 'BEGIN ### machine: %s ###' % addr
            print 'Running processes belonging to %s' % username
            print m.run_cmd('ps -u %s' % username)
            print 'free output'
            print m.run_cmd('free -m')
            print 'END ### machine: %s ###' % addr
            print '\n\n'


    addr = raw_input('Enter machine addr\ninput ALL to loop through all machines\n'\
         'input TOP <machine> to grab top from given machine > ')
    if addr == 'ALL':
        for addr, m in machine_list.iteritems():
            print_machine_details(m, addr)
    elif addr.split(' ')[0] == 'TOP':
            addr = addr.split(' ')[1]
            print machine_list[addr].run_cmd('top -n 1 -b')
    else:
        if addr in machine_list:
            print_machine_details(machine_list[addr], addr)
        else:
            print 'Invalid addr'
