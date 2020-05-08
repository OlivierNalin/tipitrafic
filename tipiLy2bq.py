#!/usr/bin/env python7
# coding: utf8
import os,time,datetime
import requests
import re
import time
import xml.dom.minidom
from time import mktime
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "pr35-c1b8856bbc0b.json"
os.environ['CLOUD_STORAGE_BUCKET'] = "bk351"
with open('tipCredentialstpC','r') as tpC:
    cred=tpC.read().split("\n")    
    user=cred[0] #"tipitrafic-pub"
    password=cred[1]    
baseurl = "http://diffusion-numerique.info-routiere.gouv.fr/tipitrafic/"
try :
    with open('last_tsl','r') as fs:
        last_ts=int(fs.read())
except:
     with open('last_tsl','w') as fs:    
        fs.write('-1')
        last_ts=-1

def process_measurement(m, ts):
    sensor = m.getElementsByTagName('measurementSiteReference')[0].getAttribute('id')
    flow = int(m.getElementsByTagName('vehicleFlow')[0].getAttribute('numberOfInputValuesUsed'))
    flog.write(sensor+','+ str(ts)+','+str(flow)+'\n')

def process_file(file):
    dom = xml.dom.minidom.parseString(file)
    pub = dom.getElementsByTagName('payloadPublication')[0]
    pubtime = (pub.getElementsByTagName('publicationTime'))[0].firstChild.data
    ts = str(int(time.mktime(datetime.datetime.strptime(pubtime, "%Y-%m-%dT%H:%M:%S").timetuple())))
    for child in pub.childNodes:
        if child.nodeType == child.ELEMENT_NODE and child.tagName == 'siteMeasurements':
            process_measurement(child, ts)

def fetch_sixmin(sixmin_url):
    fl = requests.get(sixmin_url, auth=(user,password)).text
    process_file(fl)
    with open("tipilog.csv", "rb") as my_file:
        blob.upload_from_file(my_file)
    uri = "gs://bk351/"+file
    load_job = bqclient.load_table_from_uri(uri, dataset_ref.table("impLyon"), job_config=job_config)
    load_job.result()
    with open('tipilog.csv','w') as ff:
        a=0
def fetch_day(url):
    global last_ts
    global flog
    flog=open('tipilog.csv','a')
    day = get_page_contents(url, '.xml')
    for sixmin in day:
        m = re.search('.*/TraficLyon_DataTR_([0-9-_]+)\.xml$', sixmin)        
        if m == None:
            continue
        else:
            ts = m.group(1)
            parsed_ts = int(mktime(time.strptime(ts, "%Y%m%d_%H%M%S")))           
            if parsed_ts > last_ts:
                last_ts=parsed_ts
                fetch_sixmin(sixmin)
            else:
                print( "[fetch_day] skipping sixmin " + ts)
    flog.close()
    with open('last_ts','w') as fs :fs.write(str(last_ts))
    

# fetch all the missing data
def fetch_data(network):
     # get all directories from the root    
    basedir_days = get_page_contents(baseurl + network, '/')
    for day in basedir_days:                      
        m = re.search('.*/([0-9-_]+)/$', day)
        if m == None:
            continue
        else:
            day_ts = m.group(1)            
            # 2018-07-25_09 - we append the minute '59' to get the whole hour
            parsed_ts = int(mktime(time.strptime(day_ts + '-59', '%Y-%m-%d_%H-%M')))
            if parsed_ts >= last_ts:               
                fetch_day(day)
            else:
                print( "[fetch_data] skipping " + day_ts)


# parses the output of an Apache DirectoryIndex page
# returns an array of links
def get_page_contents(url, ext):
    page = requests.get(url, auth=(user,password)).text
    soup = BeautifulSoup(page, 'html.parser')
    return [ url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext) ]

bqclient = bigquery.Client()
stclient = storage.Client()
dataset_id = 'tipiLyon'
dataset_ref = bqclient.dataset(dataset_id)
job_config = bigquery.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField("station", "STRING"),
    bigquery.SchemaField("timesta", "INTEGER"),
    bigquery.SchemaField("q", "INTEGER"),
]
bucket = stclient.get_bucket('bk351')
file ='ftipi'+'.csv'
blob = bucket.blob(file)
fetch_data('TraficLyon/')
