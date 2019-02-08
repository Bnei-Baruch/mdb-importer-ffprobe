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


query_string = 'select META().id id, enreached.audio_bit_rates[0] audio_bitrate FROM `test3` WHERE enreached.type == "audio" limit {};'.format(conf["records_to_read"])



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


with open('audio.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in cb_obj:
        a = list(i.values())
        spamwriter.writerow(a)