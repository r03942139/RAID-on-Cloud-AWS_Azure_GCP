# RAID-on-Cloud-AWS_Azure_GCP
A NAS (network-attached storage) backed by three public cloud storage services is designed.

From the perspective of customers, a single cloud provider may not be trustworthy enough for providing the best quality of service and cost-efficiency. A single-platform application suffers the “**vendor lock-in**” problem, blocking the customers from migrating to better cloud providers. More and more customers have adopted a multi-platform approach, in order to obtain better services. 

RAID-on-Cloud, a cloud-based replication NAS which we will implement in this project. The client-end will be implemented in Python, as a CLI (command-line interface) running in a Singularity Container. Through the Python CLI, users can access files without knowing how data are actually stored and distributed across cloud platforms.

Build-up Steps
1. Prepare information for accessing public cloud storages: AWS S3, Azure, GCP; The information includes:
**AWS S3**: access_key_id, access_secret_key, bucket_name
**Azure**: key, conn_str, account_name, container_name
**GCP Storage**: credential_file, bucket_name, 
Then open **RAIS-on-Cloud-AWS_Azure_GCP/cloud.py** and fill the above in.

2. Proper Python revision on the Singularity Container (Python 2.7.18)
**$ module load Python/2.7.18-GCCcore-10.2.0**

3. Run use cases:
For instance, 

**$ ./run-tests.sh tests.test_2_NAS.test_1_RAID_on_Cloud**

Another way of running the individual test cases is to use the following commands
**$ PYTHONPATH=$PWD/lib:$PWD python tests/test_1_cloud_storage.py -v **
**$ PYTHONPATH=$PWD/lib:$PWD python tests/test_1_cloud_storage.py -v test1_AWS_S3**
**$ PYTHONPATH=$PWD/lib:$PWD python tests/test_2_NAS.py -v**

4. Future Works
- the NAS Balancing
- the Open-to-close Consistency
