"""Run BQ query and write results"""

import json
import os
import re
import uuid
import base64
import time
import datetime
import dateutil.parser
import pytz

from google.cloud import bigquery
from google.cloud import logging
from google.cloud.logging import Resource
from github import Github

GCP_PROJECT = os.environ.get('GCP_PROJECT')

# logging
ERROR = 'ERROR'
WARNING = 'WARNING'
INFO = 'INFO'

GCP_FUNCTION_NAME = os.environ.get('FUNCTION_NAME')
GCP_FUNCTION_REGION = os.environ.get('FUNCTION_REGION')

log_client = logging.Client()
log_name = 'cloudfunctions.googleapis.com%2Fcloud-functions'

# Inside the resource, nest the required labels specific to the resource type
res = Resource(type="cloud_function",
               labels={
                   "function_name": GCP_FUNCTION_NAME,
                   "region": GCP_FUNCTION_REGION
               })

logger = log_client.logger(log_name.format(GCP_PROJECT))

# create bigquery client
client = bigquery.Client()

def dynamic_filter_config_generate(event, context):
    """Background Cloud Function to be triggered by Pub/Sub (pub sub message is sent by cloud scheduler).
        Args:
            event: Data passed in from pub sub message.
            context (google.cloud.functions.Context): The Cloud Functions event
            metadata.
    """

    uuidstr = str(uuid.uuid4())

    log(INFO, ("Received query request from {0}".format(context.resource)), uuidstr)

    start = current_time()

    job_config = bigquery.QueryJobConfig()
    target_time = dateutil.parser.parse(context.timestamp)

    query_request = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    minBidRate = "0.1"
    minAverageBid = "0.1"
    minCombinedBidRate = "0.3"
    minCombinedAverageBid = "0.2"
    if ("minBidRate" in query_request):
        minBidRate = query_request['minBidRate']
    if ("minAverageBid" in query_request):
        minAverageBid = query_request['minAverageBid']
    if ("minCombinedBidRate" in query_request):
        minCombinedBidRate = query_request['minCombinedBidRate']
    if ("minCombinedAverageBid" in query_request):
        minCombinedAverageBid = query_request['minCombinedAverageBid']

    sql = """
DECLARE fromDate timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS timestamp);
DECLARE toDate timestamp DEFAULT CAST(CURRENT_DATE() AS timestamp);
DECLARE defaultFilter numeric DEFAULT 0.05;
DECLARE minRequests int64 DEFAULT 5000;
DECLARE minBidRate numeric DEFAULT {0};
DECLARE minAverageBid numeric DEFAULT {1};
DECLARE minCombinedBidRate numeric DEFAULT {2};
DECLARE minCombinedAverageBid numeric DEFAULT {3};


WITH continentEntries AS (

SELECT bidder_id Bidder, geo_continent Continent,
Sum(CASE WHEN bidder_status = "bid" THEN 1 ELSE 0 END) Bids,
Count(1) Total,
Avg(CASE WHEN bidder_status = 'bid' THEN bidder_cpm ELSE NULL END) Average_Bid
FROM `streamamp-production.raw_events.pbs_a`
where geo_continent IS NOT NULL
AND geo_continent != 'unknown'
AND error_code != "500"
AND time > fromDate AND time < toDate
GROUP BY bidder_id, geo_continent
HAVING (
    (Bids/Total < minCombinedBidRate AND Average_Bid < minCombinedAverageBid)
    OR Bids/Total < minBidRate
    OR Average_Bid < minAverageBid
)
AND Total > minRequests

),

allCountries AS (

SELECT bidder_id Bidder, geo_continent Continent, geo_country Country,
Sum(CASE WHEN bidder_status = "bid" THEN 1 ELSE 0 END) Bids,
Count(1) Total,
Avg(CASE WHEN bidder_status = 'bid' THEN bidder_cpm ELSE NULL END) Average_Bid
FROM `streamamp-production.raw_events.pbs_a`
where geo_continent IS NOT NULL
AND geo_country IS NOT NULL
AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND error_code != "500"
AND time > fromDate AND time < toDate
GROUP BY bidder_id, geo_continent, geo_country

),

countryEntries AS (

SELECT Bidder, Continent, Country, Bids, Total, Average_Bid
FROM allCountries
WHERE Continent NOT IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = allCountries.Bidder)
AND (
    (Bids/Total < minCombinedBidRate AND Average_Bid < minCombinedAverageBid)
    OR Bids/Total < minBidRate
    OR Average_Bid < minAverageBid
)
AND Total > minRequests

),

allRegions AS (

SELECT bidder_id Bidder, geo_continent Continent, geo_country Country, geo_region Region,
Sum(CASE WHEN bidder_status = "bid" THEN 1 ELSE 0 END) Bids,
Count(1) Total,
Avg(CASE WHEN bidder_status = 'bid' THEN bidder_cpm ELSE NULL END) Average_Bid
FROM `streamamp-production.raw_events.pbs_a`
where geo_continent IS NOT NULL
AND geo_region IS NOT NULL
AND geo_country IS NOT NULL
AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND geo_region != 'unknown'
AND error_code != "500"
AND time > fromDate AND time < toDate
GROUP BY bidder_id, geo_continent, geo_country, geo_region

),

regionEntries AS (

SELECT Bidder, Continent, Country, Region, Bids, Total, Average_Bid
FROM allRegions 
WHERE Continent NOT IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = allRegions.Bidder)
AND Country NOT IN (SELECT Country FROM countryEntries WHERE countryEntries.Bidder = allRegions.Bidder)
AND (
    (Bids/Total < minCombinedBidRate AND Average_Bid < minCombinedAverageBid)
    OR Bids/Total < minBidRate
    OR Average_Bid < minAverageBid
)
AND Total > minRequests

)

SELECT Bidder, Continent, Country, Region, Host, Filter
FROM (

SELECT Bidder, Continent, null AS Country, null AS Region, null AS Host, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM continentEntries

UNION ALL

SELECT Bidder, Continent, null AS Country, null AS Region, Host, Bids, Total, Average_Bid, 1 AS Filter
FROM
(
SELECT bidder_id Bidder, geo_continent Continent, host AS Host,
Sum(CASE WHEN bidder_status = "bid" THEN 1 ELSE 0 END) Bids,
Count(1) Total,
Avg(CASE WHEN bidder_status = 'bid' THEN bidder_cpm ELSE NULL END) Average_Bid
FROM `streamamp-production.raw_events.pbs_a` AS auctions
where geo_continent IS NOT NULL
AND geo_continent != 'unknown'
AND error_code != "500"
AND time > fromDate AND time < toDate
AND geo_continent IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = auctions.bidder_id)
GROUP BY bidder_id, geo_continent, host
HAVING (
    Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
    OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
)
AND Total > minRequests
)

UNION ALL

SELECT Bidder, Continent, Country, null AS Region, null AS Host, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM countryEntries

UNION ALL 

SELECT Bidder, Continent, Country, null AS Region, null AS Host, Bids, Total, Average_Bid, 1 AS Filter
FROM allCountries  
WHERE Continent IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = allCountries.Bidder)
AND (
    Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
    OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
)
AND Total > minRequests

UNION ALL

SELECT Bidder, Continent, Country, null AS Region, Host, Bids, Total, Average_Bid, 1 AS Filter
FROM
(
SELECT bidder_id Bidder, geo_continent Continent, geo_country Country, host AS Host,
Sum(CASE WHEN bidder_status = "bid" THEN 1 ELSE 0 END) Bids,
Count(1) Total,
Avg(CASE WHEN bidder_status = 'bid' THEN bidder_cpm ELSE NULL END) Average_Bid
FROM `streamamp-production.raw_events.pbs_a` AS auctions
where geo_continent IS NOT NULL
AND geo_country IS NOT NULL
AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND error_code != "500"
AND time > fromDate AND time < toDate
AND geo_country IN (SELECT Country FROM countryEntries WHERE countryEntries.Bidder = auctions.bidder_id)
GROUP BY bidder_id, geo_continent, geo_country, host
HAVING (
    Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
    OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
)
AND Total > minRequests
)

UNION ALL

SELECT Bidder, Continent, Country, Region, null AS Host, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM regionEntries

UNION ALL 

SELECT Bidder, Continent, Country, Region, null AS Host, Bids, Total, Average_Bid, 1 AS Filter
FROM allRegions 
WHERE Country IN (SELECT Country FROM countryEntries WHERE countryEntries.Bidder = allRegions.Bidder)
AND (
    Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
    OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
)
AND Total > minRequests

UNION ALL

SELECT Bidder, Continent, Country, Region, Host, Bids, Total, Average_Bid, 1 AS Filter
FROM
(
SELECT bidder_id Bidder, geo_continent Continent, geo_country Country, geo_region Region, host AS Host,
Sum(CASE WHEN bidder_status = "bid" THEN 1 ELSE 0 END) Bids,
Count(1) Total,
Avg(CASE WHEN bidder_status = 'bid' THEN bidder_cpm ELSE NULL END) Average_Bid
FROM `streamamp-production.raw_events.pbs_a` AS auctions
where geo_continent IS NOT NULL
AND geo_country IS NOT NULL
AND geo_region IS NOT NULL
AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND geo_region != 'unknown'
AND error_code != "500"
AND time > fromDate AND time < toDate
AND geo_region IN (SELECT Region FROM regionEntries WHERE regionEntries.Bidder = auctions.bidder_id)
GROUP BY bidder_id, geo_continent, geo_country, geo_region, host
HAVING (
    Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
    OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
)
AND Total > minRequests
)

)
ORDER BY Bidder, Continent, Country, Region, Host
    """.format(minBidRate, minAverageBid, minCombinedBidRate, minCombinedAverageBid)

    #Run query
    log(INFO, "Executing query", uuidstr)
    try:
        # Start the query
        query_job = client.query(
            sql)  # API request - starts the query

        results = query_job.result()

        log(INFO, "Job {0} is currently in state {1}".format(query_job.job_id, query_job.state), uuidstr)

        jsn = {}
        current_bidder = ''
        current_continent = ''
        current_country = ''
        current_region = ''
        filter_list = {}
        continent_entry = {}
        country_entry = {}
        region_entry = {}

        for row in results:
            if (row.Bidder != current_bidder):
                if (current_bidder != ''):
                    jsn[current_bidder] = filter_list.copy()
                filter_list = {}
                current_bidder = row.Bidder
            if (current_continent != row.Continent):
                if (current_country != ''):        
                    if (len(country_entry) == 1):
                        continent_entry[current_country] = country_entry["default"]
                    else:
                        continent_entry[current_country] = country_entry.copy()
                if (current_continent != ''):
                    if (len(continent_entry) == 1):
                        filter_list[current_continent] = continent_entry["default"]
                    else:
                        filter_list[current_continent] = continent_entry.copy()
                continent_entry = {}
                current_continent = row.Continent
                country_entry = {}
                region_entry = {}
                current_country = ''
                current_region = ''
                if (row.Country == None and row.Host == None):
                    continent_entry["default"] = float(row.Filter)
                else:
                    continent_entry["default"] = 1
            if (row.Country != None):
                if (current_country != row.Country):
                    if (current_country != ''):
                        if (len(country_entry) == 1):
                            continent_entry[current_country] = country_entry["default"]
                        else:
                            continent_entry[current_country] = country_entry.copy()
                    country_entry = {}
                    current_country = row.Country
                    region_entry = {}
                    current_region = ''
                    if (row.Region == None and row.Host == None):
                        country_entry["default"] = float(row.Filter)
                    else:
                        country_entry["default"] = 1
                if (row.Region != None):
                    if (current_region != row.Region):
                        if (current_region != ''):
                            if (len(region_entry) == 1):
                                country_entry[row.Region] = region_entry["default"]
                            else:
                                country_entry[row.Region] = region_entry.copy()
                        region_entry = {}
                        current_region = row.Region
                        if (row.Host == None):
                            region_entry["default"] = float(row.Filter)
                        else:
                            region_entry["default"] = 1
                    if (row.Host != None):
                        region_entry[row.Host] = float(row.Filter)
                elif (row.Host != None):
                    country_entry[row.Host] = float(row.Filter)
            elif (row.Host != None):
                continent_entry[row.Host] = float(row.Filter)
        if (current_region != ''):        
            if (len(region_entry) == 1):
                country_entry[current_region] = region_entry["default"]
            else:
                country_entry[current_region] = region_entry.copy()
        if (current_country != ''):        
            if (len(country_entry) == 1):
                continent_entry[current_country] = country_entry["default"]
            else:
                continent_entry[current_country] = country_entry.copy()
        if (len(continent_entry) == 1):
            filter_list[current_continent] = continent_entry["default"]
        else:
            filter_list[current_continent] = continent_entry.copy()
        jsn[current_bidder] = filter_list.copy()
        
        filename = "qa-dynamic-filter-list.json"
        if ("filename" in query_request):
            filename = query_request['filename']
        
        g = Github(query_request['githubKey'])
        repo = g.get_repo("t13s2s/config")
        contents = repo.get_contents(filename, ref="main")
        git_response = repo.update_file(contents.path, "Cloud Function Commit", json.dumps(jsn, indent = 4), contents.sha, branch="main")

        log(INFO, "Commit to Github: {}".format(str(git_response)), uuidstr)
        
        print(jsn)

    except Exception as e:
        log(ERROR, ('Failed to run query job. Retrying, error: {0}'.format(e)), uuidstr)
        raise

    if(query_job.errors):
        log(ERROR, ('Query job request with error(s): {0}'.format(str(query_job.errors))), uuidstr)

def log(severity, msg, uuid, op='dynamic_filter_config_generate'):
    data = {'op': op, 'msg': ("'{0}'".format(msg)), 'uuid': uuid}

    logger.log_struct(
        {"message": json.dumps(data)}, resource=res, severity=severity)

def current_time():
    return int(round(time.time() * 1000))
