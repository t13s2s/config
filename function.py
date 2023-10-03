"""Run BQ query and write results"""

import json
import os
import uuid
import base64
import time
import dateutil.parser
import dynamicfilter

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

    # query_request = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    # minBidRate = "0.1"
    # minAverageBid = "0.1"
    # minCombinedBidRate = "0.3"
    # minCombinedAverageBid = "0.2"
    # if ("minBidRate" in query_request):
    #     minBidRate = query_request['minBidRate']
    # if ("minAverageBid" in query_request):
    #     minAverageBid = query_request['minAverageBid']
    # if ("minCombinedBidRate" in query_request):
    #     minCombinedBidRate = query_request['minCombinedBidRate']
    # if ("minCombinedAverageBid" in query_request):
    #     minCombinedAverageBid = query_request['minCombinedAverageBid']

    sql = """
DECLARE from_date_ext timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) AS timestamp);
DECLARE from_date timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS timestamp);
DECLARE to_date timestamp DEFAULT CAST(CURRENT_DATE() AS timestamp);
DECLARE default_filter numeric DEFAULT 0.05;
DECLARE min_requests int64 DEFAULT 5000;

with all_country_entries as(
  SELECT bidder_id bidder, geo_continent continent,geo_country country,
  sum(ifnull(bids,0)) bids,
  sum(ifnull(bid_requests,0)) total_bids,
  sum(ifnull(case when error_code is null then total_top_bids end,0)) total_top_bids,
  round(safe_divide(sum(ifnull(case when error_code is null then total_top_bids end,0)),sum(ifnull(case when error_code is null then bid_requests end,0)))*100,2) top_bid_rate,
  case when sum(ifnull(bid_requests,0))>min_requests then 'high' else 'low' end request_volume,
  FROM `freestar-prod.prebid_server_raw.unified_events_daily`
  where record_date >= from_date AND record_date < to_date
  AND geo_continent IS NOT NULL
  AND geo_country IS NOT NULL
  AND geo_continent != 'nan'
  AND geo_country != 'nan'
  AND bidder_id IS NOT NULL
  AND (error_code IS NULL or error_code ='500')
  group by 1,2,3
  ),



country_entries_treated as (
  select bidder, continent,
  CASE WHEN total_bids<=min_requests and country!='RU' THEN 'default' ELSE country END country,
  sum(ifnull(a.bids,0)) bids, 
  sum(ifnull(a.bid_requests,0)) total_bids,
  sum(ifnull(case when error_code is null then a.total_top_bids end,0)) total_top_bids,
  round(safe_divide(sum(ifnull(case when error_code is null then a.total_top_bids end,0)),sum(ifnull(case when error_code is null then a.bid_requests end,0)))*100,2) top_bid_rate,
   from `freestar-prod.prebid_server_raw.unified_events_daily` a
  join all_country_entries b on a.bidder_id=b.bidder and a.geo_continent=b.continent and a.geo_country=b.country
  where request_volume='low' and (error_code IS NULL or error_code ='500')
  and record_date >= from_date_ext AND record_date < to_date
  group by 1,2,3

  union all

  select * except(request_volume) from all_country_entries where request_volume='high'
  ),


threshold_type as(

  select continent,country,
  round(max(top_bid_rate) - min(top_bid_rate),1) range_,
  count(distinct(bidder)) bidders_above_5,
  case when round(max(top_bid_rate) - min(top_bid_rate),1)>10 then 'variable_threshold' else 'fixed_threshold' end method 
  from country_entries_treated
  where top_bid_rate>5 
  group by 1,2
  ),


countries_thr_cal as(
  select bidder, continent,country,range_,bidders_above_5, method,
  sum(c.total_bids) over(partition by continent,country,bidder) total_bids,
  avg(c.top_bid_rate) over(partition by continent,country,bidder) top_bid_rate,
  case when method='variable_threshold' then round((avg(top_bid_rate) over (partition by continent,country))*0.9,0) else 5 end thr
  from country_entries_treated c
  left join threshold_type using (continent,country)
  ),


country_level_results as(select bidder,continent,country, range_,bidders_above_5,method,total_bids,top_bid_rate,thr,
case 
when country='RU' then 0
when method is null then default_filter 
when top_bid_rate<5 then default_filter
when top_bid_rate<thr then default_filter
else 1.0 end filter
from countries_thr_cal
order by continent,country,top_bid_rate desc, bidder
),




host_entries as(SELECT bidder_id bidder, geo_continent continent,geo_country country,host,
  sum(ifnull(bids,0)) bids,
  sum(ifnull(bid_requests,0)) total_bids,
  sum(ifnull(case when error_code is null then total_top_bids end,0)) total_top_bids,
  round(safe_divide(sum(ifnull(case when error_code is null then total_top_bids end,0)),sum(ifnull(case when error_code is null then bid_requests end,0)))*100,2) top_bid_rate,
  -- case when sum(ifnull(bid_requests,0))>min_requests then 'high' else 'low' end request_volume,
  FROM `freestar-prod.prebid_server_raw.unified_events_daily`
  where (error_code IS NULL or error_code ='500')
  and record_date >= from_date AND record_date < to_date
  AND geo_continent IS NOT NULL
  AND geo_country IS NOT NULL
  AND bidder_id IS NOT NULL
  group by 1,2,3,4
  having sum(ifnull(bid_requests,0))>min_requests
  ),

host_thr_type as(select continent, country,host,
round(max(top_bid_rate) - min(top_bid_rate),1) range_,
count(distinct(bidder)) bidders_above_5,
case when round(max(top_bid_rate) - min(top_bid_rate),1)>10 then 'variable_threshold' else 'fixed_threshold' end method ,
from host_entries
where top_bid_rate>5 
group by 1,2,3),

host_thr_cal as(
  select bidder, continent,country,host,range_,bidders_above_5, method,
  sum(h.total_bids) over(partition by continent,country,host,bidder) total_bids,
  avg(h.top_bid_rate) over(partition by continent,country,host,bidder) top_bid_rate,
  case when method='variable_threshold' then round((avg(top_bid_rate) over (partition by continent,country,host))*0.9,0) else 5 end thr
  from host_entries h
  left join host_thr_type using (continent,country,host)
  ),

host_level_results as(
  select bidder,continent,country,host, range_,bidders_above_5,method,total_bids,top_bid_rate,thr,
case 
when country='RU' then 0
when method is null then default_filter
when top_bid_rate<5 then default_filter
when top_bid_rate<thr then default_filter
else 1.0 end filter
from host_thr_cal
order by continent,country,top_bid_rate desc, bidder),


unioned as(
select bidder,continent,country,'default' host,total_bids,top_bid_rate,thr,filter
from country_level_results

union all

select bidder,continent,country,host,total_bids,top_bid_rate,thr,filter
from host_level_results 
),

finalized as(select out.* from unioned out 
left join unioned in_ on in_.bidder=out.bidder and in_.continent=out.continent and in_.country=out.country and in_.host='default'
where out.host='default' or out.filter!=in_.filter
order by 1,2,3,4)

select bidder, continent,country,host, filter from finalized
    """

    # Run query
    log(INFO, "Executing query", uuidstr)
    try:
        # Start the query
        query_job = client.query(
            sql)  # API request - starts the query

        results = query_job.result()

        log(INFO, "Job {0} is currently in state {1}".format(query_job.job_id, query_job.state), uuidstr)

        # override = {}
        # if "override" in query_request:
        #     override = query_request["override"]

        mapped_json = dynamicfilter.map_query_results(results)

        filename = "new-traffic-shaping-dynamic-filter-list.json"
        # if "filename" in query_request:
        #     filename = query_request['filename']

        g = Github("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")
        repo = g.get_repo("t13s2s/config")
        contents = repo.get_contents(filename, ref="main")
        git_response = repo.update_file(contents.path, "Cloud Function Commit", json.dumps(mapped_json, indent=4),
                                        contents.sha, branch="main")

        log(INFO, "Commit to Github: {}".format(str(git_response)), uuidstr)

    except Exception as e:
        log(ERROR, ('Failed to run query job. Retrying, error: {0}'.format(e)), uuidstr)
        raise

    if query_job.errors:
        log(ERROR, ('Query job request with error(s): {0}'.format(str(query_job.errors))), uuidstr)


def log(severity, msg, uuid, op='dynamic_filter_config_generate'):
    data = {'op': op, 'msg': ("'{0}'".format(msg)), 'uuid': uuid}

    logger.log_struct(
        {"message": json.dumps(data)}, resource=res, severity=severity)


def current_time():
    return int(round(time.time() * 1000))
