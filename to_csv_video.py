import logging
import sys
import yaml
from couchbase.cluster import Cluster, Bucket
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql import N1QLQuery
import csv




with open("config.yml", 'r') as config_file:
    try:
        conf = (yaml.load(config_file))
    except yaml.YAMLError as e:
        print(e)


query_string = 'select META().id id, enreached.aspect_ratios[0] aspect_ratio , enreached.video_sizes[0] video_size FROM `test3` WHERE enreached.type == "video" limit {};'.format(conf["records_to_read"])



log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)
format = logging.Formatter("%(levelname)s - %(asctime)s - %(threadName)s - %(name)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)


log.info("will retrieve {} from api".format(conf["records_to_read"]))
cb = Bucket('couchbase://{cb_host}/{cb_bucket}'.format(cb_host = conf["cb_host"], cb_bucket = conf["cb_bucket"]),
                username=conf["cb_user"], password=conf["cb_pass"])
cb_obj = cb.n1ql_query(query_string)


video_names = { "320x240": "QVGA", "640x360": "nHD", "640x480": "VGA" }


with open('video.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in cb_obj:
        video_size_name = ""
        if i["video_size"] in video_names.keys():
            video_size_name = video_names[i["video_size"]]
        a = [ i["id"], i["aspect_ratio"], i["video_size"], video_size_name ]
        spamwriter.writerow(a)