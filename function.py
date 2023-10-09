"""Run BQ query and write results"""

import json
import time
import dynamicfilter
import base64
import tempfile
import os
from google.cloud import bigquery
from google.cloud import storage

import google.cloud.logging
import logging

client = google.cloud.logging.Client()
client.setup_logging()

# create bigquery client
client = bigquery.Client()

# create gcs client
gcs_client = storage.Client(project="t13s2s")


def dynamic_filter_config_generate(event, context):
    """Background Cloud Function to be triggered by Pub/Sub (pub sub message is sent by cloud scheduler).
        Args:
            event: Data passed in from pub sub message.
            context (google.cloud.functions.Context): The Cloud Functions event
            metadata.
    """

    logging.info("Received query request from {0}".format(context.resource))

    query_request = json.loads(base64.b64decode(event['data']).decode('utf-8'))

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

finalized as(select out.* 
from unioned out 
left join unioned in_ on in_.bidder=out.bidder and in_.continent=out.continent and in_.country=out.country and in_.host='default'
left join unioned in_2 on in_2.bidder=out.bidder and in_2.continent=out.continent and in_2.country='default'
where (out.host='default'  or out.filter!=in_.filter) and (out.country='default' or out.filter!=in_2.filter)
)

select bidder, continent,country,host, filter from finalized order by 1,2,3,4
    """

    # Run query
    logging.info("Executing query")
    try:
        # Start the query
        query_job = client.query(sql)  # API request - starts the query

        results = query_job.result()
        logging.info("Job {0} is currently in state {1}".format(query_job.job_id, query_job.state))

        dynamic_filters = dynamicfilter.map_query_results(results)

        if "override" in query_request:
            override = query_request["override"]
            dynamic_filters |= override

        filename = "new-traffic-shaping-dynamic-filter-list.json"
        if "filename" in query_request:
            filename = query_request["filename"]

        bucket = gcs_client.bucket("pbs-dynamic-filter")
        dynamic_filter_file = tempfile.NamedTemporaryFile(suffix='.json', delete=False)
        with open(dynamic_filter_file.name, 'w') as outfile:
            outfile.write(json.dumps(dynamic_filters, indent=4))
        blob = bucket.blob(filename)
        blob.upload_from_filename(dynamic_filter_file.name)
        delete_temporary_file(dynamic_filter_file.name)

        logging.info("Wrote file [{}] to GCS".format(filename))

    except Exception as e:
        logging.error('Failed to run query job. Retrying, error: {0}'.format(str(e)))
        raise

    if query_job.errors:
        logging.error('Query job request with error(s): {0}'.format(str(query_job.errors)))


def delete_temporary_file(filename):
    if os.path.exists(filename):
        os.remove(filename)
    else:
        print('File does not exist: {}'.format(filename))
