import boto3
import json
import logging
import TestJson

if __name__ == '__main__':    
    parameters_json = TestJson.read_json('parameters/poc.json')[0]
    print(parameters_json["enviroment"])
