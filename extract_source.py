import json


with open('access-log-raw.json') as f:
    hits = json.load(f)['hits']['hits']

    with open('access-log.json', 'w') as fw:
        for log in hits:
            json.dump(log['_source'], fw)
            fw.write('\n')
