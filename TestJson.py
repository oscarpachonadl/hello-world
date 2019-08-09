import json
import boto3


def get_objects_s3(bucket_name):
    r = boto3.resource('s3')
    bucket = r.Bucket(bucket_name)
    return list(bucket.objects.filter(Prefix=''))


def list_bucket_objects_s3(bucket_name_list):
    for b in bucket_name_list:
        list_objects = get_objects_s3(b)
        print('-- Bucket:', b)
        for o in list_objects:
            print('   + Object: ', o.key)

def fix_json_str(str_data):
    str_data = str_data.strip()
    if str_data[-1] == ',':
        str_data = '[' + str_data[:-1] + ']'
    return json.loads(str_data)

       
def read_json(file_name):
    with open(file_name) as raw_file:
        return fix_json_str(raw_file.read())


def count_json_elements(file_name):
    json_data = read_json(file_name)
    print('    + Count: ', len(list(json_data)), ' Object: ', file_name)


def count_json_elements_bucket(bucket_name):    
    bucket_objects = get_objects_s3(bucket_name)
    for bo in bucket_objects:
        try:
            s3 = boto3.client('s3')
            o = s3.get_object(Bucket=bo.bucket_name, Key=bo.key)
            json_data = fix_json_str(o['Body'].read())
            print('    + Count: ', len(list(json_data)), ' Object: ', bo.key)
        except Exception as e:
            print('Error in Bucket:', bo.bucket_name, 'Object:', bo.key, '\n    Error:', e)


            
# count_json_elements("G:\\Mi unidad\\Digital Analytics\\ArchivosDescargados\\adl-analytics-bb-occiauto-firehose-1-2019-08-01-21-13-08-2f4c12b2-dcde-4fde-a8b8-35bdcc2f3ef8")
# count_json_elements("G:\\Mi unidad\\Digital Analytics\\ArchivosDescargados\\adl-analytics-bb-occiauto-firehose-1-2019-08-01-21-07-57-327b0e21-0f19-446d-be1f-c8dcc32e1216")

# list_bucket_objects_s3(
#         ['adl-analytics-bb-occiauto', 
#         'adl-analytics-rb-pb', 
#         'adl-analytics-bb-pb', 
#         'adl-analytics-gb-pb', 
#         'adl-analytics-yb-pb'])

count_json_elements_bucket('adl-analytics-bb-occiauto')