#!/usr/bin/env python

import time
import psycopg2
import concurrent.futures
import requests
import json
import logging
import sys
import yaml
from couchbase.n1ql import N1QLQuery
from couchbase.cluster import Cluster, Bucket
from couchbase.cluster import PasswordAuthenticator
from couchbase import LOCKMODE_WAIT
import pprint
pp = pprint.PrettyPrinter(indent=2)


# load config
with open("config.yml", 'r') as config_file:
    try:
        conf = (yaml.load(config_file))
    except yaml.YAMLError as e:
        print(e)


# setup logging
log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)
format = logging.Formatter("%(levelname)s - %(asctime)s - %(threadName)s - %(name)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)



def connect_to_db(db_name=conf["pg_db"], db_user=conf["pg_user"],
                    db_host=conf["pg_host"],
                    db_password=conf["pg_pass"],
                    records_limit=conf["records_to_read"],
                    db_string=conf["pg_string"]):
    'connect to postgres db'
    try:
        conn = psycopg2.connect(dbname=db_name, user=db_user, host=db_host, password=db_password)
        cur = conn.cursor()
        cur.execute(db_string.format(records_limit))
        allFilesSha1 = cur.fetchall()

    except Exception as e:
        print(e)

    return allFilesSha1



log.info("will retrieve {} from api".format(conf["records_to_read"]))
pg_connect = connect_to_db()

# create list of ids (sha1) from postgres db
ids_only = []
for a in pg_connect:
    ids_only.append(a[3])

# connect to couchbase
cb = Bucket('couchbase://{cb_host}/{cb_bucket}'.format(cb_host = conf["cb_host"], cb_bucket = conf["cb_bucket"]),
                username=conf["cb_user"], password=conf["cb_pass"])
cb_obj = cb.n1ql_query('SELECT META().id FROM {} limit {}'.format(conf["cb_bucket"],conf["records_to_read"]))

#create list of ids (sha1) only from couchbase
cb_list_of_ids = []
for item in cb_obj:
    cb_list_of_ids.append(item["id"])
cb._close()

# get diff of ids between postgres (current) and couchbase (what we already have)
diff_list = list(set(ids_only) - set(cb_list_of_ids))
print("length of list of ids to work on is : ", len(diff_list))


counter = 0
import queue
q = queue.Queue()
q.queue = queue.deque(set(diff_list))

def get_aspect_ratio(w,h):
    'get acpect ratio from video size'
    for x in range(int(h), 1, -1):
        if w % x == 0 and h % x == 0:
            return str(int(w / x)) + "x" + str(int(h / x))

def make_new_struct(old1):
    old = json.loads(old1)
    # print("old= ", type(old))
    new_row = {
        "format": {},
        "enreached": {},
        "streams": {
            "video": [],
            "audio": [],
            "data": []
        }
    }
    
    # add format to new
    new_row["format"] = old["format"]

    # add type
    new_row["enreached"]["file_extension"] = old["format"]["filename"].split("/")[-1].split(".")[-1]
    #add all the streams
    num_of_video_streams = 0
    num_of_audio_streams = 0
    num_of_data_streams = 0

    
    for a in old["streams"]:
        if a["codec_type"] == "video":
            new_row["streams"]["video"].append(a)
            num_of_video_streams += 1
        if a["codec_type"] == "audio":
            new_row["streams"]["audio"].append(a)
            num_of_audio_streams += 1
        if a["codec_type"] == "data":
            new_row["streams"]["data"].append(a)
            num_of_data_streams += 1

    new_row["enreached"]["num_of_video_streams"] = num_of_video_streams
    new_row["enreached"]["num_of_audio_streams"] = num_of_audio_streams
    new_row["enreached"]["num_of_data_streams"] = num_of_data_streams

    try:
        if new_row["format"]["format_name"] == "image2":
            new_row["enreached"]["type"] = "image"
        elif len(new_row["streams"]["video"]) > 0:
            new_row["enreached"]["type"] = "video"
            video_sizes = set()
            aspect_ratios = set()
            for size in new_row["streams"]["video"]:
                if size["coded_height"] == 238:
                    size["coded_height"] = 240
                video_sizes.add(str(size["coded_width"]) + "x" + str(size["coded_height"]))
                aspect_ratios.add(get_aspect_ratio(size["coded_width"],size["coded_height"]))
            new_row["enreached"]["video_sizes"] = list(video_sizes)
            new_row["enreached"]["aspect_ratios"] = list(aspect_ratios)
        elif len(new_row["streams"]["video"]) == 0:
            new_row["enreached"]["type"] = "audio"
        
        # get audio bitrates
        if new_row["format"]["format_name"] != "image2":
            audio_bit_rates = set()
            for bit_rate in new_row["streams"]["audio"]:
                audio_bit_rates.add(bit_rate["bit_rate"])
            new_row["enreached"]["audio_bit_rates"] = list(audio_bit_rates)
    

    except Exception as e:
        print("aaaaaaaa",e)
    # print(new_row)
    return new_row


def apiGetFileInfo(sha):
    payload = { "sha1": sha }
    # print("payload = ",payload)
    headers = {'Content-Type': 'application/json'}
    try:
        r = requests.post( conf["api_url"], json = payload, headers = headers )
        if r.status_code != 200:
            log.error("we have status code {} on sha1 {}".format(r.status_code, sha))
        a = make_new_struct(r.text)
    except Exception as e:
        a = {}
        log.error("there was a problem getting info for sha {}, {}".format(sha, e) )


    try:
        cb = Bucket('couchbase://10.66.1.239/test3',username="Administrator", password="123456", lockmode=LOCKMODE_WAIT)
        cb.upsert(sha, a)
    except Exception as ex:
        log.error(ex)
    try:
        cb._close()
    except Exception as ex:
        log.error("can't close the bucket")
        log.error(ex)


with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    for x in range(len(diff_list)):
        executor.submit(apiGetFileInfo, str(q.get()))