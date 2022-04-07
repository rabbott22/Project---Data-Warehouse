import sys
import boto3
from botocore.exceptions import ClientError
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', 
                   region_name=region,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
        print("Bucket name " + bucket_name + " has been created.")

    except ClientError as e:
        print(e)
        return False
       
    return True

def main(argv):
    '''Take bucket name argument entered after script name on command line.
    Submit argument to create_bucket function.
    '''
    create_bucket(argv[1])

if __name__ == "__main__":
    main(sys.argv)