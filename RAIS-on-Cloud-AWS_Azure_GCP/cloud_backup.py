from basic_defs import cloud_storage, NAS
import os, uuid
import sys

import boto3 # AWS_S3
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # Azure
from google.cloud import storage # Google_Cloud_Storage


class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = "AKIAZ3WFZEEF2ARUKYJL"
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = "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"
        # TODO: Fill in the bucket name
        self.bucket_name = "csce678-s21-p1-430005499"
        self.session = boto3.Session(
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.access_secret_key,
                  )
        self.s3 = self.session.resource('s3')
        self.client = boto3.client('s3')
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

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = "u04DPr/UGGADYcl27vrXG3lAZ7cMP7LC+4Y3NKuR3nL8jLkp0xwG9NRzfCtDHG2nn4xX4adldrHfFmhRtT3afA=="
        # TODO: Fill in the Azure connection string
        self.conn_str = "DefaultEndpointsProtocol=https;AccountName=csce678s21;AccountKey=u04DPr/UGGADYcl27vrXG3lAZ7cMP7LC+4Y3NKuR3nL8jLkp0xwG9NRzfCtDHG2nn4xX4adldrHfFmhRtT3afA==;EndpointSuffix=core.windows.net"
        # TODO: Fill in the account name
        self.account_name = "csce678s21"
        # TODO: Fill in the container name
        self.container_name = "csce678-s21-p1-430005499"

        # Create the BlobServiceClient object which will be used to create a container client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)

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


class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = "gcp-credential.json"
        # TODO: Fill in the container name
        self.bucket_name = "csce678-s21-p1-430005499"
        self.client = storage.Client.from_service_account_json(self.credential_file)
        self.bucket = self.client.get_bucket(self.bucket_name)
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

class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [
                AWS_S3(),
                Azure_Blob_Storage(),
                Google_Cloud_Storage()
            ]

    # Implement the abstract functions from NAS
    #def open(self, filename):
    #    raise NotImplementedError

    #def read(self, fd, len, offset):
    #    raise NotImplementedError

    #def write(self, fd, data, offset):
        # remainder as source, quotient as offset
        # 3000/3 = 1000 ... 0
    #    raise NotImplementedError

    #def close(self, fd):
    #    raise NotImplementedError

    #def delete(self, filename):
    #    raise NotImplementedError

    #def get_storage_sizes(self):
    #    return [len(b.list_blocks()) for b in self.backends]
