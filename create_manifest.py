import boto3
import configparser
import os
import sys

def main(argv):
    """Create an AWS Redshift cluster copy manifest file

    :Python script call takes file name and bucket name arguments from command line
     to use as the manifest file name and remote bucket repository.
      
        $ python create_manifest.py <file_name> <bucket_name>
        
        e.g:
        
        $ python create_manifest.py song.manifest bobs-bucket-22
    
    :Connect to S3 data source specified in dwh.cfg file
    :Convert collection of objects to a list for processing
    :Write manifest file to current working directory
    """
    ## Parse configuratino file and assign key values to variables
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    SONG_DATA              = config.get('S3', 'SONG_DATA')

    ## Create S3 connection
    s3 = boto3.resource('s3',
                   region_name='us-west-2',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )

    ## Create S3 bucket resource
    sparkifyBucket = s3.Bucket('udacity-dend')

    ## Acquire bucket collection and convert to list
    print("Aquiring collection of song data files.")
    bucket_list = sparkifyBucket.objects.filter(Prefix='song_data/')
    print("Converting collection to list for processing.")
    song_files = list(bucket_list.all())

    end = len(song_files)
    print("Song data source = " + str(SONG_DATA))
    print("Number of song files is: " + str(end))
    print("First song file: " + str(song_files[1].key))
    print("Last song file: " + str(song_files[-1].key))

    filename = argv[1]
    
    ## Create manifest file, format list data and write to file
    with open(filename, 'w+') as man:
        man.write("{\n    \"entries\": [\n")
        song_files = iter(song_files)
        next(song_files)
        song_data = SONG_DATA.replace("'", "").replace("song_data", "")
        for obj in song_files:
            #print(obj)
            man.write("        {\"url\":\"" + song_data + obj.key + "\", \"mandatory\":true},\n")
 
    ## Remove extra comma from end and finish writing file
    with open(filename, 'rb+') as man:
        man.seek(-3, os.SEEK_END)
        man.truncate()
        man.write("\r\n    ]\r\n}".encode("utf-8"))

    print("Manifest file \"" + filename + "\" created.")

    ## Upload file from local current working director to S3 remote directory
    myBucket = s3.Bucket(argv[2])
    directory_key = "song_data/" + filename
    print("Uploading manifest file to S3 bucket: " + myBucket.name + "/" + "song_data/" + filename)
    upload = './' + filename
    s3.meta.client.upload_file(upload, myBucket.name, directory_key)
    print("Upload complete.")

if __name__ == "__main__":
    main(sys.argv)