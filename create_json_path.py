import boto3
import configparser
import os
import sys

#def main(argv):
def main():
    """Create JSONPaths file to use with Redshift COPY to load data to cluster DB.

    :Python script call takes a file name argument from command line
        to use as the manifest file name $ python <script_name> <file_name>
        e.g:
            $ python create_json_path.py song_json_path.json

    :Connect to S3 data source specified in dwh.cfg file
    :Read to a list for processing
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
                   region_name='us-east-1',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )

    ## Create S3 bucket resource
    sparkifyBucket = s3.Bucket('udacity-dend')
    #print(type(sparkifyBucket))
    ## Acquire bucket collection and convert to list
    print("Aquiring collection of song data files.")
    bucket_list = sparkifyBucket.objects.filter(Prefix='song_data/')
    print(type(bucket_list))
    filesizes = {}
    for obj in bucket_list:
        #key = sparkifyBucket.lookup(obj.key)
        #s3.head_object()
        #list_obj = s3.Object(sparkifyBucket, obj.key)
        print(type(obj.key))
        filesizes.update({obj.key : str(sparkifyBucket.Object(obj.key).content_length)})
    print("Largest song file is: " + max(filesizes, key=filesizes.get) + " = " + max(filesizes.values()))
        #print("File " + obj.key + " size = " + size + ".")
    #resources = bucket_list.get_available_subresources()
    #print(resources)
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

if __name__ == "__main__":
    #main(sys.argv)
    main()