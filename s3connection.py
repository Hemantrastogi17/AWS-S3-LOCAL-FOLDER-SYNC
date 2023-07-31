import boto3
import pandas as pd
import os
import filecmp
import hashlib

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id="AKIxxxxxxxxxxxxxxxV4",
    aws_secret_access_key="Nlxxxxxxxxxxxxxxxxxxxxxxxxxxx1"
)
SOURCE_DIR = '/Volumes/Coding Playground/S3 Bucket Data Folder'

existing_files_s3 = []


# Make a list of files which are uploaded in the S3 Bucket
# for object in s3.Bucket('firefistbucket2').objects.all():
#     # existing_files_s3.append(object)
#     obj = s3.Bucket('firefistbucket2').Object(object.key).get()
#     print(obj['ResponseMetadata']['HTTPHeaders']['etag'])


# existing_files_local= []

def check_if_file_present_in_s3(file):
    for object in s3.Bucket('firefistbucket2').objects.all():
        if (object.key == file):
            print("File already present in the S3 Bucket")
            return True;
            break;
        else:
            return False;


def get_md5(filename):
    f = open(SOURCE_DIR + '/' + file.name, 'rb')
    m = hashlib.md5()
    while True:
        data = f.read(10240)
        if len(data) == 0:
            break
        m.update(data)
    return m.hexdigest()


# Iterate through the files present in SOURCE DIRECTORY
with os.scandir(SOURCE_DIR) as files:
    for file in files:
        print(file.name)
        if (check_if_file_present_in_s3(file.name)):
            # Compare the MD5 hashes of both the files
            etag = ''
            for object in s3.Bucket('firefistbucket2').objects.all():
                # global etag
                obj = s3.Bucket('firefistbucket2').Object(object.key).get()
                etag = obj['ResponseMetadata']['HTTPHeaders']['etag']

            # print(SOURCE_DIR +'/'+file.name)
            checksum = get_md5(SOURCE_DIR + '/' + file.name)
            # checksum = hashlib.md5(open(SOURCE_DIR +'/'+file.name).read()).hexdigest()

            print(etag.strip('"'))
            if (etag.strip('"') == checksum):
                print("S3 bucket up to date")

        else:
            # Upload the file
            filename = SOURCE_DIR + "/" + file.name
            print(filename)
            s3.Bucket('firefistbucket2').upload_file(Filename=filename, Key=file.name)
