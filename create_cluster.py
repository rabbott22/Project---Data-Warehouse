import pandas as pd
import boto3
import json
import configparser
import time
import sys

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB_NAME            = config.get("DWH","DWH_DB_NAME")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

def create_iam_role (region=None):
    ## Create a new Identity and Access Management (IAM) client
    iam = boto3.client('iam',
                    region_name=region,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

    ## Create a new IAM role
    if iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['RoleName'] != DWH_IAM_ROLE_NAME:
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

    ## Assign ARN Role to a variable
    dwhRole = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    roleArn = dwhRole['Role']['Arn']
    print("IAM Role ARN: ", roleArn)
    return roleArn

def create_cluster (roleArn, region=None):
    ## Create Redshift client connection
    redshift = boto3.client('redshift',
                        region_name=region,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    ## Create Redshift Cluster
    print("Creating cluster in region " + region)
    try:
        response = redshift.create_cluster(        
            # Add parameters for hardware
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),

            # Add parameters for identifiers & credentials
            DBName = DWH_DB_NAME,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,
            
            # Add parameter for role (to allow s3 access)
            IamRoles = [roleArn]
        )
        print(type(response))
        print(response)
    except Exception as e:
        print(e)
    return redshift

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    print("Number of keys = " + str(len(keysToShow)))
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def main(argv):
    roleArn = create_iam_role()
    redshift = create_cluster(roleArn, argv[1])
    #redshift = create_cluster(roleArn)

    ## Describe the cluster to monitor for available status
    #myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    #schedule.every(2).minutes.do(prettyRedshiftProps(myClusterProps))
    #df = pd.DataFrame()
    status = None
    i = 1
    while status != "available":
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = prettyRedshiftProps(myClusterProps)
        status = df.iloc[2]['Value']
        print(str(i) + ". Cluster " + df.iloc[0]['Value'] + " status is " + status + ".")
        time.sleep(120)
        i = i + 1

    print(df)
    #print("Cluster " + df.iloc[0]['Value'] + " status is " + df.iloc[2]['Value'] + ".")

if __name__ == "__main__":
    main(sys.argv)
