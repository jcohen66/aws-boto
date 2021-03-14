import boto3
import uuid

def create_bucket_name(bucket_prefix):
    # the generated bucket name must be between 3 and 63 characters.
    return ''.join([bucket_prefix, str(uuid.uuid4())])



def create_bucket(bucket_prefix, s3_connection):
    '''
    Works with both client and resource.

    :param bucket_prefix:
    :param s3_connection:
    :return:
    '''
    # load credetals from file
    session = boto3.session.Session()
    current_region = session.region_name
    # generate a globally unique bucket name
    bucket_name = create_bucket_name(bucket_prefix)
    # create the bucket
    if current_region == 'us-east-1':
        bucket_response = s3_connection.create_bucket(
            Bucket=bucket_name
        )
    else:
        bucket_response = s3_connection.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': current_region
            }
        )
    print(bucket_name, current_region)
    return bucket_name, bucket_response

def create_temp_file(size, file_name, file_content):
    '''
    Create a nondeterministic filename prefix so that performance
    doesnt degrade when there are lots of files.
    :param size:
    :param file_name:
    :param file_content:
    :return:
    '''
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name

def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)

s3_resource = boto3.resource('s3')

first_bucket_name, first_response = create_bucket(
    bucket_prefix='firstpythonbucket',
    s3_connection=s3_resource.meta.client
)
print(first_bucket_name)

first_file_name = create_temp_file(300, 'firstfile.txt', 'f')

# If you have a bucket, you can get an object.
first_bucket = s3_resource.Bucket(name=first_bucket_name)
first_object = s3_resource.Object(bucket_name=first_bucket_name, key=first_file_name)

# If you have an object, you can get a bucket.
first_object_again = first_bucket.Object(first_file_name)
first_bucket_again = first_object.Bucket()

# Upload a file using an Object instance.
s3_resource.Object(first_bucket_name, first_file_name).upload_file(
    Filename=first_file_name
)

first_object.upload_file(first_file_name)

# Upload a file using a Bucket instance.
s3_resource.Bucket(first_bucket_name).upload_file(
    Filename=first_file_name, Key=first_file_name
)

# Upload using a client.
s3_resource.meta.client.upload_file(
    Filename=first_file_name, Bucket=first_bucket_name,
    Key=first_file_name
)

# Download a file.
s3_resource.Object(first_bucket_name, first_file_name).download_file(
    f'/tmp/{first_file_name}'
)

second_bucket_name, second_response = create_bucket(
    bucket_prefix='secondpythonbucket',
    s3_connection=s3_resource.meta.client
)
print(second_bucket_name)

# Upload  with ACL.
second_file_name = create_temp_file(400, 'secondfile.txt', 's')
second_object = s3_resource.Object(first_bucket_name, second_file_name)
second_object.upload_file(second_file_name,
                          ExtraArgs={"ACL": "public-read"}
                          )

# Get the object ACL instance from the Object.                          )
second_object_acl = second_object.Acl()

print(second_file_name)
print(second_object_acl.grants)

response = second_object_acl.put(ACL='private')
print(second_object_acl.grants)

# Copy from one bucket to another in the same region.
copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)

# Delete an Object.
s3_resource.Object(second_bucket_name, first_file_name).delete()

# Create a new file and upload it using Server Side Encryption.
third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
third_object = s3_resource.Object(first_bucket_name, third_file_name)
third_object.upload_file(third_file_name, ExtraArgs={
    'ServerSideEncryption': 'AES256'
})
print(third_object.server_side_encryption)

'''
Storage Classes:

STANDARD: default for frequently accessed data
STANDARD_IA: for infrequently used data that needs to be retrieved rapidly when requested.
ONEZONE_IA: same as STANDARD_IA but uses only 1 zone rather than 3 zones.
REDUCED_REDUNDANCY: for frequently used noncritical data that is easily reproducable.
'''

# Reupload a file with a different storage class.
third_object.upload_file(third_file_name, ExtraArgs={
    'ServerSideEncryption': 'AES256',
    'StorageClass': 'STANDARD_IA'
})

# If you change the remote object, you must reload the local object.
third_object.reload()
print(third_object.storage_class)

'''
Versioning:

When you add a new version of an object, the storage that 
object takes in total is the sum of the size of its versions. 
So if you’re storing an object of 1 GB, and you create 10 
versions, then you have to pay for 10GB of storage.
'''

def enable_bucket_versioning(bucket_name):
    bkt_versioning = s3_resource.BucketVersioning(bucket_name)
    bkt_versioning.enable()
    print(bkt_versioning.status)

enable_bucket_versioning(first_bucket_name)

# Create new versions of the first file, one original
# contents and one with contents of the third file.
s3_resource.Object(first_bucket_name, first_file_name).upload_file(first_file_name)
s3_resource.Object(first_bucket_name, first_file_name).upload_file(third_file_name)

# Reupload the second file, which will create a new version.
s3_resource.Object(first_bucket_name, second_file_name).upload_file(third_file_name)

# Retrieve the latest available version of the object.
print(s3_resource.Object(first_bucket_name,first_file_name).version_id)


# Traverse all of the buckets.
for bucket in s3_resource.buckets.all():
    print(bucket.name)

# Traverse all of the buckets and get a dict of metadata.
for bucket_dict in s3_resource.meta.client.list_buckets().get('Buckets'):
    print(bucket_dict['Name'])

# Traverse the objects in a particular bucket. Obj is a lightweight summary of the object.
for obj in first_bucket.objects.all():
    print(obj.key)

# Traverse using the Object Subresource.
for obj in first_bucket.objects.all():
    subsrc = obj.Object()
    print(obj.key, obj.storage_class, obj.last_modified, subsrc.version_id, subsrc.metadata)


'''
Delete Buckets:

Buckets must be empty before they can be deleted.
'''

def delete_all_objects(bucket_name):
    '''
    To be able to delete a bucket, you must first delete every single object within the bucket, 
    or else the BucketNotEmpty exception will be raised. When you have a versioned bucket, 
    you need to delete every object and all its versions.
    
    :param bucket_name:
    :return: 
    '''
    res = []
    bucket = s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key, 'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})

delete_all_objects(first_bucket_name)

# Upload to second bucket without versioning enabled.
s3_resource.Object(second_bucket_name, first_file_name).upload_file(first_file_name)
delete_all_objects(second_bucket_name)

# Delete the bucket instance (must be empty first).
s3_resource.Bucket(first_bucket_name).delete()

# Delete the bucket instance using the client instance (must be empty first).
s3_resource.meta.client.delete_bucket(Bucket=second_bucket_name)


