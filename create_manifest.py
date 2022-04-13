import boto3
import configparser
import os
import sys

def main(argv):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    SONG_DATA              = config.get('S3', 'SONG_DATA')

    s3 = boto3.resource('s3',
                   region_name='us-east-1',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
                   )

    sparkifyBucket = s3.Bucket('udacity-dend')

    print("Aquiring collection of song data files.")
    bucket_list = sparkifyBucket.objects.filter(Prefix='song_data/')
    print("Converting collection to list.")
    song_files = list(bucket_list.all())

    end = len(song_files)
    print("Song data source = " + str(SONG_DATA))
    print("Number of song files is: " + str(end))
    print("First song file: " + str(song_files[1].key))
    print("Last song file: " + str(song_files[-1].key))

    filename = argv[1]
    
    with open(filename, 'w+') as man:
        man.write("{\n    \"entries\": [\n")
        song_files = iter(song_files)
        next(song_files)
        song_data = SONG_DATA.replace("'", "").replace("song_data", "")
        for obj in song_files:
            #print(obj)
            man.write("        {\"url\":\"" + song_data + obj.key + "\", \"mandatory\":true},\n")
        #print("File handle at: ", man.tell())
 
    with open(filename, 'rb+') as man:
        #print("File handle now at: ", man.tell())
        man.seek(-3, os.SEEK_END)
        #print("File handle now at: ", man.tell())
        #print(man.read(2).decode("utf-8"))
        #print("File handle now at: ", man.tell())
        man.seek(-3, os.SEEK_END)
        man.truncate()
        man.write("\r\n    ]\r\n}".encode("utf-8"))

    print("Manifest file \"" + filename + "\" created.")

if __name__ == "__main__":
    main(sys.argv)