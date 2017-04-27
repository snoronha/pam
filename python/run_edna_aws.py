#!/usr/bin/env python
import boto3
import time
from subprocess import Popen, PIPE

def get_running_instances(client):
	instances = []
	for instance in client.instances.all():
		# print "Private IP: ", instance.private_ip_address
		if instance.tags is not None and instance.state['Name'] == "running":
			for tags in instance.tags:
				if tags["Key"] == 'Name':
					instance_tag = tags["Value"]
					instances.append({'id': instance.id, 'tag': instance_tag})
	return instances

def is_running(client, instance_id):
	instance = client.Instance(instance_id)
	return instance.tags is not None and instance.state['Name'] == "running"

def create_instances(client, n, tag_prefix):
	instances = []
	ec2s = client.create_instances(
		ImageId='ami-5b039c3b',
		MinCount=n,
		MaxCount=n,
		KeyName="huff_dev_west",
		InstanceType="t2.micro"
	)
	tag_num = 0
	for instance in ec2s:
		instance_tag = tag_prefix + "_" + str(tag_num)
		client.create_tags(
			Resources = [instance.id],
			Tags = [{'Key': 'Name', 'Value': instance_tag}]
		)
		instances.append({'id': instance.id, 'tag': instance_tag})
		tag_num += 1
	return instances

def stop_instances(client, instance_ids):
	response = ec2.stop_instances(
		InstanceIds=instance_ids
	)
	return response

def terminate_instances(client, instance_ids):
	response = ec2.terminate_instances(
		InstanceIds=instance_ids
	)
	return response

def run_remote_command(host, os_cmd):
	cmd        = "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i /home/ubuntu/.ssh/huff_dev_west.pem ubuntu@" + host + " \"" + os_cmd + "\""
	print "Running: ", cmd
	p          = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
	out, err   = p.communicate()
	if p.returncode not in [0, 1]:
		print "Re-running: ", cmd
		time.sleep(5)
		p        = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
		out, err = p.communicate()
	return out, err, p.returncode
	

#-------------------------------
client        = boto3.resource('ec2')
"""
instances     = get_running_instances(client)
print "Instances currently running: ", instances
tag_prefix    = raw_input('Enter a experiment tag prefix: ')
new_instances = create_instances(client, 1, tag_prefix)
up_count      = 0
while up_count < len(new_instances):
	up_count = 0
	for instance in new_instances:
		if is_running(client, instance['id']):
			up_count += 1
	print "Number of workers started: %d of %d" % (up_count, len(new_instances))
	if up_count < len(new_instances):
		time.sleep(30)
print "Sleeping for 1 minute to allow instances to warm up ..."
time.sleep(60)
print "Experiment started with these instances: ", new_instances
"""

new_instances = [{'id': 'i-0791b714eee2a4690', 'tag': 'boogie_5_0'}]
# Divvy the problem up
MAX_FILE_COUNT = 79999
file_count     = 0
for instance in new_instances:
	ip_address = client.Instance(instance['id']).private_ip_address
	os_cmd     = "cd /home/ubuntu/go/src/anomaly; nohup ../../bin/anomaly 0 20 monthly aws > nohup0.out 2>&1&"
	out, err, return_code = run_remote_command(ip_address, os_cmd)
	print "ReturnCode: %d, Output: [%s], OutputLen: %d, Err: [%s]" % (return_code, out.rstrip(), len(out.rstrip()), err.rstrip())

done = False
for instance in new_instances:
	ip_address = client.Instance(instance['id']).private_ip_address
	while not(done):
		print "Process running ..."
		time.sleep(60)
		os_cmd     = "ps auwwx | grep anomaly | grep -v grep"
		out, err, return_code = run_remote_command(ip_address, os_cmd)
		print "ReturnCode: %d, Output: [%s], OutputLen: %d, Err: [%s]" % (return_code, out.rstrip(), len(out.rstrip()), err.rstrip())
		if return_code in [0, 1] and len(out.rstrip()) == 0:
			done = True
