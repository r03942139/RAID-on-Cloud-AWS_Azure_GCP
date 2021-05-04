from basic_defs import cloud_storage, NAS
import os, uuid
import sys
import math

import boto3 # AWS_S3
import botocore
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # Azure
from google.cloud import storage # Google_Cloud_Storage

import hashlib # hashcode

class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = "" #== To fill
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = "" #== To fill
        # TODO: Fill in the bucket name
        self.bucket_name = "" #== To fill
        self.session = boto3.Session(
                            aws_access_key_id=self.access_key_id,
                            aws_secret_access_key=self.access_secret_key,
                        )
        self.s3 = self.session.resource('s3')
        # self.client = boto3.client('s3')
        #bucket = s3.create_bucket(Bucket=self.bucket_name)

    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from boto3
    #     boto3.session.Session: (client to s3) -> access_key_id, access_secret_key
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    #     boto3.resources: [V] (s3)
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    #     boto3.s3.Bucket: [V] (open bucket)
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket
    #     boto3.s3.Object: [V] (key-value)
    #         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object

    # bucket -> Object
    # store object to buckets
    # bucket: key->val
    def list_blocks(self):
        list = []
        my_bucket = self.s3.Bucket(self.bucket_name)
        for my_bucket_object in my_bucket.objects.all():
            list.append(int(my_bucket_object.key))
        return list
        #raise NotImplementedError

    def read_block(self, offset):
        try:
            self.s3.Object(self.bucket_name, str(offset)).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return bytearray()
        obj = self.s3.Object(self.bucket_name, str(offset))
        block = obj.get()['Body'].read()
        return bytearray(block)
        #raise NotImplementedError

    def write_block(self, block, offset):
        obj = self.s3.Object(self.bucket_name, str(offset))
        obj.put(Body=bytearray(block))
        #raise NotImplementedError

    def delete_block(self, offset):
        obj = self.s3.Object(self.bucket_name, str(offset))
        obj.delete()
        #raise NotImplementedError
     
    def is_block_exist(self, offset):
        list = []
        my_bucket = self.s3.Bucket(self.bucket_name)
        for my_bucket_object in my_bucket.objects.all():
            list.append(my_bucket_object.key)
        if str(offset) in list: return True
        return False

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = "" #== To fill
        # TODO: Fill in the Azure connection string
        self.conn_str = "" #== To fill
        # TODO: Fill in the account name
        self.account_name = "" #== To fill
        # TODO: Fill in the container name
        self.container_name = "" #== To fill

        # Create the BlobServiceClient object which will be used to create a container client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)

        ###### Clear the Container ######
        '''
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob.name)
            blob_client.delete_blob()
        '''
    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from azure.storage.blob
    #    blob.BlobServiceClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python
    #    blob.ContainerClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.containerclient?view=azure-python
    #    blob.BlobClient:
    #        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python

    # Container -> block
    def list_blocks(self):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_list = container_client.list_blobs()
        list = []
        for blob in blob_list:
            list.append(int(blob.name))
        return list

    def read_block(self, offset):
        # offset: 0,      1,
        #         0-4095 4096-...
        # key of Container = offset
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_client = container_client.get_blob_client(str(offset))
        block = blob_client.download_blob().readall() # read blob content as string
        return bytearray(block)

    def write_block(self, block, offset):
        blob = BlobClient.from_connection_string(conn_str=self.conn_str, container_name=self.container_name, blob_name=str(offset))
        blob.upload_blob(bytearray(block), overwrite=True)
        # https://stackoverflow.com/questions/62604352/how-can-i-directly-upload-a-string-to-azure-storage-blob-using-the-python-sdk?fbclid=IwAR3oPlCOLlhGvqxKKcvIpzhMgtgv43vbSnZFDrO0SbOxlpkIsYMq1g2f_5w

    def delete_block(self, offset):
        # Create a blob client using the local file name as the name for the blob
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=str(offset))
        blob_client.delete_blob()

    def is_block_exist(self, offset):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blob_list = container_client.list_blobs()
        list = []
        for blob in blob_list:
            list.append(blob.name)
        if str(offset) in list: return True
        return False

class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = "" #== To fill
        # TODO: Fill in the container name
        self.bucket_name = "" #== To fill
        self.client = storage.Client.from_service_account_json(self.credential_file)
        self.bucket = self.client.get_bucket(self.bucket_name)
        
        ###### Clear the bucket ######
        #for blob in self.client.list_blobs(self.bucket_name):
        #    blob.delete()
	##########`


    # Implement the abstract functions from cloud_storage
    # Hints: Use the following APIs from google.cloud.storage
    #    storage.client.Client:
    #        https://googleapis.dev/python/storage/latest/client.html
    #    storage.bucket.Bucket:
    #        https://googleapis.dev/python/storage/latest/buckets.html
    #    storage.blob.Blob:
    #        https://googleapis.dev/python/storage/latest/blobs.html

    # Bucket -> Blob
    def list_blocks(self):
        # list of offset
        list = []
        for blob in self.client.list_blobs(self.bucket_name):
            list.append(int(blob.name))
        return list

    def read_block(self, offset):
        blob = self.bucket.get_blob(str(offset))
        block = blob.download_as_string()
        return bytearray(block)

    def write_block(self, block, offset):
        blob = self.bucket.blob(str(offset)) # 
        blob.upload_from_string(str(block)) # 

    def delete_block(self, offset):
        blob = self.bucket.blob(str(offset)) 
        blob.delete()

    def is_block_exist(self, offset):
        list = []
        for blob in self.client.list_blobs(self.bucket_name):
            list.append(blob.name)
        if str(offset) in list: return True
        return False

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]
        self.block_size = 4096
        self.fds = dict()
            
    # 3000/3 = 1000 ... 0 => S3, Azure
    # 3001/3 = 1000 ... 1 => Azure, GCP
    # 3002/3 = 1000 ... 2 => GCP, S3
    
    # Implement the abstract functions from NAS
    def open(self, filename):
        newfd = None
        for fd in range(4096):
            if fd not in self.fds:
                newfd = fd
                break
        if newfd is None:
            raise IOError("Opened files exceed system limitation.")
        self.fds[newfd] = filename
        return newfd

    def read(self, fd, len, offset):
        # If the file does not exist in the cloud return bytearray()
        if fd not in self.fds: return bytearray()
        filename = self.fds[fd]
        # Check the filename0 block, to confirm if file exist?
        key0, cloud0, cloud1 = self.get_md5sum(filename + str(0))
        if self.backends[cloud0].is_block_exist(key0) == False: return bytearray()
        
        # the offset has exceeded the end of the file, return remaining part
        # start and end indexes of blocks
        #start_idx = offset // self.block_size
        end_idx = (offset+len) // self.block_size
        
        data = bytearray()
        for idx in range(0, end_idx+1): # [0, end_idx]
            key, cloud0, cloud1 = self.get_md5sum(filename + str(idx))
            if self.backends[cloud0].is_block_exist(key) == False:
                break
            data = data + self.backends[cloud0].read_block(key)

        return data[offset:offset+len]

    def write(self, fd, data, offset):
        # remainder as source, quotient as offset
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        filename = self.fds[fd]
        
        # Decide the block locations to write
        start_idx = offset // self.block_size
        end_idx = (offset+len(data)-1) // self.block_size
        
        # Check the filename0 block, to confirm if file exist?
        key0, cloud0, cloud1 = self.get_md5sum(filename + str(0))
        if self.backends[cloud0].is_block_exist(key0) == False:
            data_to_write = bytearray([0] * offset) + data # 0x00 as filler value
            start_idx = 0
        else:
            data_to_write = self.read(fd, offset+len(data), 0)
        
        data_to_write[offset: offset+len(data)] = data

        # Split data based on unit of self.block_size
        block_list = [""] * (end_idx-0+1)
        end_location = offset + len(data)
        for idx in range(0, end_idx+1):
            #print(min(end_location, self.block_size*idx + self.block_size))
            block_list[idx] = data_to_write[self.block_size*idx+0: min(end_location, self.block_size*idx + self.block_size)]
        
        for idx in range(0, end_idx+1): # [0, end_idx]
            key, cloud0, cloud1 = self.get_md5sum(filename + str(idx))
            '''
            print("###")
            print(filename + str(idx) + "  " + str(key) + "  " + str(cloud0))
            print(data)
            print("###")
            '''
            block = block_list[idx]
            self.backends[cloud0].write_block(block, key)
            self.backends[cloud1].write_block(block, key)
        
        return
    
    def delete(self, filename):
        # Check the filename0 block, to confirm if file exist?
        key0, cloud0, cloud1 = self.get_md5sum(filename + str(0))
        if self.backends[cloud0].is_block_exist(key0) == False: return
        
        # while loop until block not found
        idx = 0
        while 1:
            key, cloud0, cloud1 = self.get_md5sum(filename + str(idx))
            if self.backends[cloud0].is_block_exist(key) == False: break
            if self.backends[cloud1].is_block_exist(key) == False: break
            self.backends[cloud0].delete_block(key)
            self.backends[cloud1].delete_block(key)
            idx = idx + 1
        return
    
    def close(self, fd):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)
        del self.fds[fd]
        return

    # return key, cloud0, cloud1
    def get_md5sum(self, filename):
        hashcode = int(hashlib.md5(filename).hexdigest(), 16) % 433494437 # 433494437 is a prime
        key = hashcode // 3
        cloud0 = hashcode % 3
        cloud1 = (cloud0 + 1) % 3
        return key, cloud0, cloud1

