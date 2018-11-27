#!/usr/bin/env python
#coding=utf8

import sys
import os,json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

import time
from common import MNSCommon
# Import MNS Library to call messaging service API
from mns.account import Account
from mns.queue import *
# Import Batch compute library to trigger batch compute job using API
from batchcompute import Client, ClientError
from batchcompute.resources import (
    JobDescription, TaskDescription, DAG,
    GroupDescription, ClusterDescription,
)
from batchcompute import CN_SHENZHEN as REGION 

#Batch Job JSON configurations template

job_desc = """{
  "Name": "RunJob-%s",
  "Type": "DAG",
  "JobFailOnInstanceFail": true,
  "Description": "",
  "Priority": 0,
  "DAG": {
    "Tasks": {
      "T1": {
        "ClusterId": "",
        "InstanceCount": 1,
        "MaxRetryCount": 0,
        "Parameters": {
          "Command": {
            "CommandLine": "python %s.py",
            "PackagePath": "oss://mybatchjobscripts/%s.tar.gz"
          },
          "InputMappingConfig": {
            "Lock": true
          },
          "StderrRedirectPath": "oss://mybatchjobscripts/log/",
          "StdoutRedirectPath": "oss://mybatchjobscripts/output/"
        },
        "AutoCluster": {
          "ResourceType": "OnDemand",
          "Configs": {
            "Networks": {
              "VPC": {
                "CidrBlock": "192.168.0.0/16"
              }
            },
            "Mounts": {
              "CacheSupport": true,
              "Entries": []
            },
            "Disks": {
              "DataDisk": {},
              "SystemDisk": {
                "Size": 40,
                "Type": ""
              }
            }
          },
          "ReserveOnFail": false,
          "ImageId": "img-ubuntu",
          "InstanceType": "ecs.sn1.medium"
        },
        "WriteSupport": true,
        "Timeout": 180,
        "Mounts": {
          "Lock": false,
          "CacheSupport": false
        }
      }
    },
    "Dependencies": {}
  },
  "Notification": {
    "Topic": {
      "Name": "",
      "Events": []
    }
  }
}"""

#Load config values
accid,acckey,endpoint,token = MNSCommon.LoadConfig()

#Instantiate batchcompute client
client=Client(REGION, accid, acckey)

# my_account, my_queue
my_account = Account(endpoint, accid, acckey, token)
if len(sys.argv) > 1:
 queue_name = sys.argv[1]
else:
 sys.stderr.write("Please specify queue name while executing the script. (python recvdelmessage.py <queue name>)")

base64 = False if len(sys.argv) > 2 and sys.argv[2].lower() == "false" else True
my_queue = my_account.get_queue(queue_name)
my_queue.set_encoding(base64)


#Read delete message until the queue is empty
#The receive message request uses the long polling mode, and the long polling time is 3 seconds by wait_seconds.

## long polling Resolution:
### When there is a message in the queue, the request returns immediately;
### When there is no message in the queue, the request hangs on the MNS server for 3 seconds. During this period, a message is written to the queue, and the request will return the message immediately. After 3 seconds, the request returns to the queue without a message.

wait_seconds = 3
print "%sReceive And Processing Message From Queue%s\nQueueName:%s\nWaitSeconds:%s\n" % (10*"=", 10*"=", queue_name, wait_seconds)
while True:
    #Read message
    try:
        recv_msg = my_queue.receive_message(wait_seconds)
        print "Receive Message Succeed! ReceiptHandle:%s MessageBody:%s MessageID:%s" % (recv_msg.receipt_handle, recv_msg.message_body, recv_msg.message_id)
    except MNSExceptionBase,e:
        if e.type == "QueueNotExist":
            print "Queue not exist, please create queue before receive message."
            sys.exit(0)
        elif e.type == "MessageNotExist":
            print "Queue is empty!"
            #sys.exit(0)
        print "Receive Message Fail! Exception:%s\n" % e
        continue

    #Call Batch Job Start Script
    try:
        # Prepare the job_json_object depending on the received message
        if recv_msg.message_body.lower() == "job1":
            job_json_argument = job_desc % ("job1","Script1","job1")
        elif recv_msg.message_body.lower() == "job2":
            job_json_argument = job_desc % ("job2","Script2","job2")
        elif recv_msg.message_body.lower() == "job3":
            job_json_argument = job_desc % ("job3","Script3","job3")
        else:
            continue

        print job_json_argument
        job_id = client.create_job(json.loads(job_json_argument)).Id
        print "Job has been successfully submitted %s", job_id
        my_queue.delete_message(recv_msg.receipt_handle)
        print "Delete Message Succeed!  ReceiptHandle:%s" % recv_msg.receipt_handle
        #Comment below line if you want script to continue process multiple messages
        sys.exit(0) 
    except ClientError, e:
        print (e.get_status_code(), e.get_code(), e.get_requestid(), e.get_msg()) 
    except Exception,e:
        print "Delete Message Fail! Exception:%s\n" % e
        sys.exit(0)
