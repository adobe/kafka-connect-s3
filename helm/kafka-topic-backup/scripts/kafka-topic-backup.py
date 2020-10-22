"""
Copyright 2020 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
"""

import requests
import yaml
import sys
import json
import os

url_prefix=os.getenv("CONNECT_URL", "http://localhost:8083")
conf_file="/tmp/config/kafka-topic-backup.yaml"

def connector_exists(topic):
    print("Check {} exists".format(topic))

    r = requests.get('{}/connectors/{}/status'.format(url_prefix,topic))
    return 0 if r.status_code == 404 else 1

def connector_update_config(topic, topic_configs):
    print("Update connector config for {}".format(topic))
    
    connector_config = {
        "topics":"{}".format(topic),
        "s3.prefix":"{}".format(topic),
    }
    global_config={k:v for k,v in topic_configs.items()}
    connector_config.update(global_config)

    r = requests.put("{}/connectors/{}/config".format(url_prefix,topic), 
        data=json.dumps(connector_config), 
        headers={"Content-Type":"application/json"})
    print("Connect response {}".format(r.text))

    return 0 if r.status_code == 200 else 1

def connector_create(topic, topic_configs):
    print("Create connector for {}".format(topic))

    connector = {
        "name":"{}".format(topic),
        "config": {
            "topics":"{}".format(topic),
            "s3.prefix":"{}".format(topic)
        }
    }
    global_config={k:v for k,v in topic_configs.items()}
    connector["config"].update(global_config)

    r = requests.post("{}/connectors".format(url_prefix), 
        data=json.dumps(connector), 
        headers={"Content-Type":"application/json"})
    print("Connect response {}".format(r.text))
    
    return 0 if r.status_code == 200 else 1


def backup_topic(topic, topic_configs):

    if connector_exists(topic):
        return connector_update_config(topic, topic_configs)
    else:
        return connector_create(topic, topic_configs)

if __name__=="__main__":

    err_code=0
    with open(conf_file,"r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)
            sys.exit(1)


        global_config = config["global"]
        topics = config["topics"]

        for topic in topics:
            topic_configs = global_config.copy()
            topic_configs.update(topics[topic])
            err_code += backup_topic(topic,topic_configs)

    sys.exit(err_code)
