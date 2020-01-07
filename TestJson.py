import boto3
import json
import os
import logging
import click
import parameters.param


def get_objects_s3(bucket_name: str):
    """
    Get objects from s3 bucket in aws
    :param bucket_name: the name of the bucket to get objects
    """
    object_list = []
    # boto3.Session(profile_name='983903018317_rb-datalake-dataengineer')
    r = boto3.resource('s3')
    try:
        bucket = r.Bucket(bucket_name)
        object_list = list(bucket.objects.filter(Prefix=''))
    except Exception as e:
        logging.warning("Can't access Bucket: %s \n. Message: %s", bucket_name, e)
    finally:
        return object_list


def list_bucket_objects_s3(bucket_name_list: list):
    """
    Get objects from s3 buckets in aws specified in the list
    :param bucket_name_list: the list of the names of the buckets on s3
    """
    for b in bucket_name_list:
        list_objects = get_objects_s3(b)
        # -- print('-- Bucket:', b)
        logging.info('-- Bucket: %s', b)
        for o in list_objects:
            # print('   + Object: ', o.key)
            logging.info('   + Object: %s', o.key)

def fix_json_str(str_data):
    if not type(str_data) is str:
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
    logging.info('    + Count: %s Object: %s', len(list(json_data)), file_name)
    # print('    + Count: ', len(list(json_data)), ' Object: ', file_nae)


def count_json_elements_bucket(bucket_name: str, download: bool, files_directory: str):    
    bucket_objects = get_objects_s3(bucket_name)
    objects_count = len(bucket_objects)
    bucket_name_print = '== Bucket: ' + bucket_name + ' [Objects: ' + str(objects_count) + '] ' 
    logging.info(bucket_name_print.ljust(120, '='))
    # print(bucket_name_print.ljust(120, '='))
    
    count = 0
    for bo in bucket_objects:
        try:
            new_object = False
            new_object_string = ''
            s3 = boto3.client('s3')
            o = s3.get_object(Bucket=bo.bucket_name, Key=bo.key)
            json_data = fix_json_str(o['Body'].read())
            count_o = len(list(json_data))
            # download the object to local file
            if download :
                file_name = files_directory + bo.key.split('/')[-1]
                if not os.path.isfile(file_name) :
                    s3.download_file(bo.bucket_name, bo.key, file_name)
                    new_object = True    
                
            if new_object :
                new_object_string = '(New)'

            logging.info('    +%s Count: %s Object: %s', new_object_string, count_o, bo.key)
            # print('    +', new_object_string, 'Count: ', count_o, ' Object: ', bo.key)
            count += count_o
        except Exception as e:
            logging.error('Problem in Bucket: %s Object %s \nMessage: %s', bo.bucket_name, bo.key, e)
    return count, objects_count


def count_json_elements_buckets(bucket_name_list: str, download: bool = False, files_directory: str = "G:\\Mi unidad\\Digital Analytics\\ArchivosDescargados\\"):
    total_count = 0
    total_objects_count = 0
    for b in bucket_name_list:
        elements_count, objects_count = count_json_elements_bucket(b, download, files_directory)
        total_count += elements_count
        total_objects_count += objects_count
    logging.info('** Total elements: %s', total_count)
    logging.info('** Total objects: %s', total_objects_count)
    # print('** Total elements:', total_count)
    # print('** Total objects:', total_objects_count)


@click.command()
@click.option('--env', '-e', default=['poc'], type=click.Choice(parameters.param.ENVIRONMENTS), 
    help="Environment for run the program", multiple=True)
@click.option('--download/--no-download', default=False,
    help="Download S3 objets to local file")
@click.option("--aws_access_key_id",default="",
    help="AWS Key ID")
@click.option("--aws_secret_access_key",default="",
    help="AWS Secret key")

def main(env: list =[], aws_access_key_id: str ="", aws_secret_access_key: str ="", download: bool =False):
        list_env = list(env)
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

        if list_env.count('all') > 0:
            list_env = parameters.param.ENVIRONMENTS 
            
        for e in list_env:            
            # if e != 'poc':
                # os.environ["AWS_ACCESS_KEY_ID"] = 'ASIA6KFJQTVG4ECDDIX7'
                # os.environ["AWS_SECRET_ACCESS_KEY"] = '4Ff3LbpNnkGR5Lb4et3FZBjWhywsGVUXvFQ3yJrt'
                # os.environ["AWS_SESSION_TOKEN"] = 'AgoJb3JpZ2luX2VjEPb//////////wEaCXVzLWVhc3QtMSJIMEYCIQCl3My+7m87+b9Y7S/Sb5CuIrcWtAfoufkTPt5QrtCfaAIhAMEZ4WLzNZQ+hTOTl+HU+lh26pNnd/Yc+zeUydTo0Wt/Ku0CCK///////////wEQABoMOTgzOTAzMDE4MzE3IgytBoo3LAz4fXcu4WIqwQLpsPp7pkxHe5bFa6jmlpf6ZwkLNCSulGxyp5bOveHc4Wl3k/w1qXbeUvfCf+0bnE+ZN8mzS7zhQMt5Mju2Ea47KxcFHIkhZvaRCfJObMAMMUlEqhtU/bAMWoHbADlxvS2qIkVPmZqGxNl7rYbkvj7/gENWIKZEcLAkvZ3C15YF1P+nvkiSzgHmeeJr6CUg48l73cW/Po0crhzpyJX/6ssXrIusW1Oxv+yJMkDmI1T8Iv9UrC4GX37BeVQ7F5kYJAvoqvplk70i+kRvsPnvdn86ZF2AfiTgIQyuim2tlPNSgIs5qhdEm91KM4YVMsvOcQ18brQZTBMg/Q8e3aerIh9SBKpSCiWgAUFdMDujBnMDg4ucbBxxmJlIilR9riW0wIU/KEYd9+Znkj7TIuTUrcMFTeZ495lciPjKLatMBNAzzMUwkZbb6wU6swGNzV2t9TrnLRa/XBkEfhTiQU1E1NuRmOL/+UQy9CXjLCVWD+3ESFAvqjxu4kWeoNEbCH//5kNcKtubRb0maQmtLEaaZiPyGZFIihrHkEwZg3fLgWmbN/V6E88D9+k/AxuZxyInf7e9oal5z7LjW3a0kijQqVNcrOKHoOMBimuACUD2vGbUki3Qieh/Ee1XTNOU/pBKkncSYoaT3XOs4ok8T13EJ5T9PtLbpu7tk3pxccFzRA=='

            parameters_json = read_json('parameters/{0}.json'.format(e))[0]
            count_json_elements_buckets(parameters_json['sourceBuckets'] , download)


if __name__ == '__main__':    
    main()