#!/usr/bin/env python3
# coding: utf8
import os
import requests
import re
import time
import xml.dom.minidom
import strict_rfc3339
from time import mktime
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "tipi-86946826-0e05b229f916.json"
os.environ['CLOUD_STORAGE_BUCKET'] = "bk36"
user="tipitrafic-pub"
password='WWWXXX'
baseurl = "http://diffusion-numerique.info-routiere.gouv.fr/tipitrafic/"
fileGS ='ftipi.csv'

def process_measurement(m, ts):
    sensor = m.getElementsByTagName('measurementSiteReference')[0].getAttribute('id')
    #print(m.getElementsByTagName('measurementSiteReference')[0],m.getElementsByTagName('vehicleFlow'))
    flow = int(m.getElementsByTagName('vehicleFlow')[0].getAttribute('numberOfInputValuesUsed'))
    flog.write(sensor+','+ str(ts)+','+str(flow)+'\n')

def process_file(file):
    dom = xml.dom.minidom.parseString(file)
    pub = dom.getElementsByTagName('payloadPublication')[0]
    pubtime = (pub.getElementsByTagName('publicationTime'))[0].firstChild.data
   
    ts = str(int(strict_rfc3339.rfc3339_to_timestamp(pubtime)))
    for child in pub.childNodes:
        if child.nodeType == child.ELEMENT_NODE and child.tagName == 'siteMeasurements':
            process_measurement(child, ts)

def fetch_sixmin(sixmin_url):
    xml = requests.get(sixmin_url, auth=(user,password)).text
    process_file(xml)
    
        
def fetch_day(url):
    global last_ts
    print("working on " + url)
    day = get_page_contents(url, '.xml')
    for sixmin in day:
        m = re.search('.*/frmar_DataTR_([0-9-_]+)\.xml$', sixmin)        
        if m == None:
            continue
        else:
            ts = m.group(1)
            parsed_ts = int(mktime(time.strptime(ts, "%Y%m%d_%H%M%S")))
            if parsed_ts > last_ts:
                fetch_sixmin(sixmin)
                last_ts=parsed_ts
            else:
                print( "[fetch_day] skipping sixmin " + ts)   

def fetch_data(network = 'TraficMarius'):
     # get all directories from the root
    basedir_days = get_page_contents(baseurl + network, '/')
    for day in basedir_days:        
        print('day in basedir_days',day)        
        m = re.search('.*/([0-9-_]+)/$', day)
        if m == None:
            continue
        else:
            day_ts = m.group(1)
             # 2018-07-25_09 - we append the minute '59' to get the whole hour
            parsed_ts = int(mktime(time.strptime(day_ts + '-59', '%Y-%m-%d_%H-%M')))
            if parsed_ts > last_ts:
                fetch_day(day)
            else:
                print( "[fetch_data] skipping " + day_ts)

# parses the output of an Apache DirectoryIndex page
# returns an array of links
def get_page_contents(url, ext):
    page = requests.get(url, auth=(user,password)).text
    soup = BeautifulSoup(page, 'html.parser')
    return [ url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext) ]
#`tipi-86946826.mars.MarsSta`
bqclient = bigquery.Client()
stclient = storage.Client()
dataset_id = 'mars'
dataset_ref = bqclient.dataset(dataset_id)
job_config = bigquery.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField("station", "STRING"),
    bigquery.SchemaField("timesta", "INTEGER"),
    bigquery.SchemaField("q", "INTEGER"),
]
bucket = stclient.get_bucket('bk36')

blob = bucket.blob(fileGS)
query_job = bqclient.query(""" 
    SELECT max(timesta) as last from `tipi-86946826.mars.impMars` """)
results = query_job.result()
for r in results :   last_ts =r.last-9
print('calculé last_ts :' , last_ts)
tmpfile='mlog.csv'

with open(tmpfile,'a') as flog:
    fetch_data()
with open(tmpfile, "rb") as my_file:
    blob.upload_from_file(my_file)
uri = "gs://bk36/"+fileGS
load_job = bqclient.load_table_from_uri(uri, dataset_ref.table("impMars"), job_config=job_config)
load_job.result() 
with open(tmpfile,'w') as ff:
    ff.write('')

sql = """
create or replace table `tipi-86946826.mars.axMars`as
select dt,
       sum(cast(axe = 'A50' as int64)*qt) as A50,
       sum(cast(axe = 'A51' as int64)*qt) as A51,    
       sum(cast(axe = 'A55' as int64)*qt) as A55,
       sum(cast(axe = 'A7' as int64)*qt) as A7      
       from (select dt,qt,axe ,cpt
FROM (select dt,sum(q) as qt,count(*) as cpt,a1
          FROM   (SELECT datetime_add(DATETIME_TRUNC(datetime(TIMESTAMP_SECONDS( timesta )),hour),interval 2 hour) as dt, 
               q, substr( station,3,1) as a1 
              FROM (select distinct * from `tipi-86946826.mars.impMars` )
              )
          group by dt,a1 ) as tb    
    join `tipi-86946826.mars.jxMars` as tj
    on tb.a1=tj.a1
    where cpt >100)
group by dt
order by dt;
"""
query_job = bqclient.query(sql)  # Make an API request. , job_config=job_config
query_job.result()  # Wait for the job to complete.
sql = """
   DECLARE mdt date;
   DECLARE mdj date;
   SET mdt = extract(date from timestamp_seconds((select max(timesta) from `tipi-86946826.mars.impMars` ))) ;
   SET mdj =  date_add((select max(EXTRACT(date FROM TIMESTAMP_SECONDS(timesta))) from `tipi-86946826.mars.tbUn` ),interval 1 day) ;
   IF (mdt>mdj) then 
       insert into `tipi-86946826.mars.tbUn` (station, timesta,q)
       select distinct station,timesta,q from `tipi-86946826.mars.impMars`
                    where  EXTRACT(date FROM TIMESTAMP_SECONDS(timesta))=mdj;
END IF;    
   
"""
query_job = bqclient.query(sql)  # Make an API request. , job_config=job_config
query_job.result()  # Wait for the job to complete.


sql = """
DECLARE mdt date;
DECLARE mdj date;
SET mdt = extract(date from timestamp_seconds((select max(timesta) from `tipi-86946826.mars.impMars` ))) ;
SET mdj =  date_add((select max(jrMs) from `tipi-86946826.mars.tbTrfJ0` ),interval 1 day) ;
IF (mdt>mdj) then 
  insert into `tipi-86946826.mars.tbTrfJ0` (jrMs,station, trfJr,cpt)
  SELECT mdj,station,TrfJr,cpt  
           FROM (SELECT station, sum(q) as  trfJr, count(*) as cpt, 
                  FROM (select distinct station,timesta,q from `tipi-86946826.mars.tbUn`
                         where  EXTRACT(date FROM TIMESTAMP_SECONDS(timesta))=mdj)
        group by station);
END IF; 
create or replace table `tipi-86946826.mars.tbTrfJ` as
select extract(day from jrMs) as jour, extract(month from jrMs) as mois, station, trfJr, cpt, jrMs, lib, axe, pr,pm
  FROM  `tipi-86946826.mars.tbTrfJ0` as a join  `tipi-86946826.mars.MarsSta`  as b
  on a.station =b.tipi ;
"""
query_job = bqclient.query(sql)  # Make an API request. , job_config=job_config
query_job.result()  # Wait for the job to complete.



