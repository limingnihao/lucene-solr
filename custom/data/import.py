import json
import base64
import random
import requests

import_path = 'school_28610.json'

solr_url = 'http://localhost:8981/solr/school/update?_=1586762171199&commitWithin=1000&overwrite=true&wt=json'

# 读取文件为json对象
def readFileToJson():
    try:
        file_object = open(import_path,encoding='utf-8')
        all_the_text = file_object.read()
        jsonObject = json.loads(all_the_text)
        return jsonObject
    finally:
        file_object.close()

# 保存到solr
def insertToSolr(res):
    aheaders = {'Content-Type': 'application/json;charset=utf-8'}
    auths = requests.auth.HTTPBasicAuth('solr', 'SolrRocks')
    for i in range(len(res)) :
        datas = []
        datas.append(res[i])
        response = requests.post(solr_url, headers=aheaders, auth=auths, data = json.dumps(datas))
        print(response)


insertToSolr(readFileToJson())




