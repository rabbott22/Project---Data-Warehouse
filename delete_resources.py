import time
import boto3
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

def delete_cluster(redshift):
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    print("Number of keys = " + str(keysToShow.count(8)))
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def delete_role(iam):
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

def main():
    ## Create Redshift client connection
    redshift = boto3.client('redshift',
                        region_name='us-east-1',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    
    ## Create a new Identity and Access Management (IAM) client
    iam = boto3.client('iam',
                    region_name=region,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

    delete_cluster(redshift)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    df = prettyRedshiftProps(myClusterProps)

    i = 0
    while df.iloc[2]['Value'] == "deleting":
        print(str(i) + ". Cluster " + df.iloc[0]['Value'] + " status is " + df.iloc[2]['Value'] + ".")
        time.sleep(120)
        df = prettyRedshiftProps(myClusterProps)
        i = i + 1

    print("Cluster " + df.iloc[0]['Value'] + " status is " + df.iloc[2]['Value'] + ".")

    delete_role(iam)

if __name__ == "__main__":
    main()