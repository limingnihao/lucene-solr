import json
import base64
import random
import requests
import os


json_path = '/custom/data/school_28610.json'

solr_url = 'http://localhost:8981/solr/school/update?commitWithin=1000&overwrite=true&wt=json'

aheaders = {'Content-Type': 'application/json;charset=utf-8'}

auths = requests.auth.HTTPBasicAuth('solr', 'SolrRocks')

# 读取文件为json对象
def readFileToJson():
    try:
        file_path = os.getcwd() + json_path
        file_object = open(file_path, encoding='utf-8')
        line_all = []
        while 1:
            lines = file_object.readline()
            if not lines:
                break
            line_all.append(lines)
        return line_all
    finally:
        file_object.close()

# 保存到solr
def insertToSolr(lines):
    for l in lines:
        jsonstring   = "[" + l + "]"
        response = requests.post(solr_url, headers=aheaders, auth=auths, data = jsonstring.encode("utf-8"))
        print(response)
    print('-------------over')

insertToSolr(readFileToJson())
