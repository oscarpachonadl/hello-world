import json
import boto3


def get_objects_s3(bucket_name: str):
    """
    Get objects from s3 bucket in aws
    :param bucket_name: the name of the bucket to get objects
    """
    r = boto3.resource('s3')
    bucket = r.Bucket(bucket_name)
    return list(bucket.objects.filter(Prefix=''))


def list_bucket_objects_s3(bucket_name_list: list):
    """
    Get objects from s3 buckets in aws specified in the list
    :param bucket_name_list: the list of the names of the buckets on s3
    """
    for b in bucket_name_list:
        list_objects = get_objects_s3(b)
        print('-- Bucket:', b)
        for o in list_objects:
            print('   + Object: ', o.key)

def fix_json_str(str_data):
    str_data = str_data.decode().strip()
    if str_data[-1] == ',':
        str_data = str_data[:-1]
    str_data = '[' + str_data + ']'
    return json.loads(str_data)

       
def read_json(file_name):
    with open(file_name) as raw_file:
        return fix_json_str(raw_file.read())


def count_json_elements(file_name):
    json_data = read_json(file_name)
    print('    + Count: ', len(list(json_data)), ' Object: ', file_name)


def count_json_elements_bucket(bucket_name):    
    bucket_objects = get_objects_s3(bucket_name)
    objects_count = len(bucket_objects)
    bucket_name_print = '== Bucket: ' + bucket_name + ' [Objects: ' + str(objects_count) + '] ' 
    print(bucket_name_print.ljust(120, '='))
    
    count = 0
    for bo in bucket_objects:
        try:
            s3 = boto3.client('s3')
            o = s3.get_object(Bucket=bo.bucket_name, Key=bo.key)
            json_data = fix_json_str(o['Body'].read())
            count_o = len(list(json_data))
            print('    + Count: ', count_o, ' Object: ', bo.key)
            count += count_o
        except Exception as e:
            print('Error in Bucket:', bo.bucket_name, 'Object:', bo.key, '\n    Error:', e)
    return count, objects_count


def count_json_elements_buckets(bucket_name_list):
    total_count = 0
    total_objects_count = 0
    for b in bucket_name_list:
        elements_count, objects_count = count_json_elements_bucket(b)
        total_count += elements_count
        total_objects_count += objects_count
    print('** Total elements:', total_count)
    print('** Total objects:', total_objects_count)


#Ejecución
count_json_elements_buckets(
            ['adl-analytics-bb-occiauto', 
            'adl-analytics-rb-pb', 
            'adl-analytics-bb-pb', 
            'adl-analytics-gb-pb', 
            'adl-analytics-yb-pb'])
