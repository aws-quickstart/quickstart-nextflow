import boto3
import botocore
import time
import hashlib
batchClient = boto3.client('batch')
s3Client = boto3.resource('s3')
logClient = boto3.client('logs')

class cloudWatchTail:

    
    logMessages = {}    
    
    def printAndEnrichMessage(self, message):
        print(message)
        
    
        
    def tailCloudwatchLog(self, logGroup, logStream):
                
        logs_batch = logClient.get_log_events(logGroupName=logGroup, logStreamName=logStream, startFromHead=True)

        for event in logs_batch['events']:   
            eventHashObj = hashlib.md5(b"".join([str.encode(str(event['timestamp'])), str.encode(event['message'])]))
            hashDigest = eventHashObj.hexdigest()

            if hashDigest not in self.logMessages:
                self.logMessages[hashDigest] = event['message']
                self.printAndEnrichMessage(event['message'])    
        while 'nextToken' in logs_batch:
            logs_batch = logClient.get_log_events(logGroupName=group_name, logStreamName=stream, nextToken=logs_batch['nextToken'])
            for event in logs_batch['events']:
                eventHashObj = hashlib.md5(b"".join([str.encode(str(event['timestamp'])), str.encode(event['message'])]))
                hashDigest = eventHashObj.hexdigest()

                if hashDigest not in self.logMessages:
                    self.logMessages[hashDigest] = event['message']
                    self.printAndEnrichMessage(event['message'])                

    
    def startTail(self, headNodeJobId):
        self.logMessages = {}
        currentStatus = ''
        lastStatus = ''
        
        while True:
            jobResponse = batchClient.describe_jobs(
                jobs=[headNodeJobId]
            )    
            matchingJobs = jobResponse['jobs']
            matchingJob = matchingJobs.pop(0)   
            if matchingJob['status'] == 'RUNNABLE' :
                currentStatus = 'Waiting for head job to start...'    
            if matchingJob['status'] == 'RUNNING':        
                currentStatus = "Head job is running..."
                self.tailCloudwatchLog( '/aws/batch/job', matchingJob['container']['logStreamName'])
            if matchingJob['status'] == 'SUCCEEDED' or matchingJob['status'] == 'FAILED': 
                print("Head job {0}".format(matchingJob['status']))
                self.tailCloudwatchLog( '/aws/batch/job', matchingJob['container']['logStreamName'])
                break
            if currentStatus != lastStatus:
                print(currentStatus)
                lastStatus = currentStatus
            time.sleep(1) 


