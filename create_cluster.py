import pandas as pd
import boto3
import json
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

#DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
#DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
#DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

#DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
#DWH_DB                 = config.get("DWH","DWH_DB")
#DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
#DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
#DWH_PORT               = config.get("DWH","DWH_PORT")

#DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

#pd.DataFrame({"Param":
#                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
#             "Value":
#                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
#             })

## Create S3 Client
s3 = boto3.resource('s3',
                   region_name='us-east-1',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )

## Examine source data on S3 staging. 
sampleDbBucket = s3.Bucket('udacity-dend')

# Create a collection object from a list of Bucket objects filtered on the specified Prefix identifier,
# then iterate through the list, print each item to screen and output to file.
'''bll = open('bucket_list_log.txt', 'w')
bll.write("bucket_name = " + sampleDbBucket.name + "\n")
objs = sampleDbBucket.objects.filter(Prefix='log_data/')
for obj in objs:
    print(obj)
    bll.write(obj.key + "\n")

bls = open('bucket_list_song.txt', 'w')
bls.write("bucket_name = " + sampleDbBucket.name + "\n")
objs = sampleDbBucket.objects.filter(Prefix='song_data/')
for obj in objs:
    print(obj)
    bls.write(obj.key + "\n")
'''

## Create a new Identity and Access Management (IAM) client
iam = boto3.client('iam',
                   region_name='us-east-1',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )

## Create a new IAM role
try:
    print("Creating a new IAM Role")
    dwhRole = iam.create_role(
        Path = '/',
        RoleName = DWH_IAM_ROLE_NAME,
        Description = "Provides Redshift ReadOnly access to S3 bucket.",
        AssumeRolePolicyDocument = json.dumps(
            {'Statement':[{'Action': 'sts:AssumeRole',
                          'Effect': 'Allow',
                          'Principal':{'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
        )
except Exception as e:
    print(e)

## Attach Policy to the newly created IAM Role
print('Attaching Policy')
iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                       PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']