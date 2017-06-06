#!/usr/bin/python
# -*- coding: utf-8 -*-
from functools import reduce
import boto3
import sys
import os
import time
import glob
import subprocess
client=boto3.resource("ec2")
#get directory from arguments
if len(sys.argv) < 3:
    verb = False
    directory = sys.argv[1]
else:
    verb = True
    directory = sys.argv[2]
#create dictinaries for commands and dependencies
commands = {}
dependencies = {}

def toposort(dependencies):
    #Union dependencies then remove nodes which are dependent rest is independent nodes
    independentNodes = reduce(set.union, dependencies.values()) - set(dependencies.keys())
    #Add this independent nodes to list
    dependencies.update({node: set() for node in independentNodes})
    while True:
        #if dependencies of a node is empty we can add it to levelNodes because it has no dependency now
        levelNodes = set(node for node, dependencies in dependencies.items() if not dependencies)
        #if levelNodes is empty break while
        if not levelNodes:
            break
        #return this levelNodes as sorted to get inputs as assistant desire
        yield ' '.join(sorted(levelNodes))
        #remove these levelNodes from dependencies list and dependencies of nodes
        dependencies = {node: (dependencies - levelNodes) for node, dependencies in dependencies.items() if node not in levelNodes}

def amazonInstance(directory, commands, sortedList):
    #create instance on amazon cloud machine
    client.create_instances(ImageId='ami-0044b96f', MinCount=1, MaxCount=1, KeyName="wdf", InstanceType='t2.micro', SecurityGroups=['wdf'])
    if verb :
        print("Instance is created")
    #wait for instance become running
    time.sleep(60)
    #Get running instances
    instances = client.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    #Keep ids of running instances
    ids=[];
    #pem file to access
    pemfile = '-i wdf.pem '
    #ssh string to make ssh connection
    sshString = r'ssh -o StrictHostKeyChecking=no '
    #Get instance in instances
    for instance in instances:
        #Get current intance's id to terminate it later
        ids.append(instance.id)
        fileName = directory.split("/")
        machine = 'ec2-user@' + instance.public_dns_name
        compress = 'tar -czvf ' + directory + '.tar.gz -C ' + directory + " . "
        extract = ' tar -xvf ' + fileName[len(fileName)-1] + '.tar.gz'
        #copy file to cloud machine
        copy = "scp -o StrictHostKeyChecking=no -i wdf.pem -r " + directory + " ec2-user@" + instance.public_dns_name + ":~"
        #copy operation
        os.system(copy)
        if verb:
            print("Workflow transferred to instance")
        #execute programs according to toposort
        if verb:
            print("Workflow execution started")
        for process in sortedList:
            process = commands.get(process)
            if verb:
                print(process + " is executed")
            os.system(sshString + pemfile + machine + " " + fileName[len(fileName)-1] + "/" + process)
            if verb:
                print(process + " is finished")
        if verb:
            print("workflow execution ended")
        #move out file to our folder
        os.system(sshString + pemfile + machine + " mv out.txt " + fileName[len(fileName)-1] + "/")
        #copy from cloud machine
        copyFrom = "scp -o StrictHostKeyChecking=no -i wdf.pem -r ec2-user@" + instance.public_dns_name + ":~/" + fileName[len(fileName)-1] + " " + directory
        os.system(copyFrom)
        if verb:
            print("Workflow is downloaded from instance")
        #Terminate instance
        client.instances.filter(InstanceIds=ids).terminate()
        if verb:
            print("instance terminated")
        #move taken files to old directory
        os.system("mv " + directory + "/" + fileName[len(fileName)-1] + "/* " + directory)
        #remove new created directory
        os.system("rmdir " + directory + "/" + fileName[len(fileName)-1])

#find wdf file
myfile = glob.glob(directory+"/*wdf")
#open file from this directory
file = open(myfile[0], "r")
#Read lines from file and fill dictinaries
for line in file:
    str = line.strip().split(":",1)
    #break if line is %%
    if "%%" in str:
        break
    commands.update({str[0]:str[1].strip()})
#print commands.get("A")
for line in file:
    str = line.replace(" ","").strip().split('=>',1)
    #if node is already exist add its new dependency to its existing set
    if str[1] in dependencies:
        dependencies.get(str[1]).add(str[0])
    else:
        dependencies.update({str[1]:set(str[0])})
#Add spaces amoung strings which come from generator and split them to add sortedList array
sortedList = ' '.join(toposort(dependencies)).split(" ")
#for process in sortedList:
#    process = commands.get(process)
#    os.system(directory+"/"+process)
#print (sortedList)
amazonInstance(directory, commands, sortedList)