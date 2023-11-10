DECLARE from_date_ext timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AS timestamp);
DECLARE from_date timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS timestamp);
DECLARE to_date timestamp DEFAULT CAST(CURRENT_DATE() AS timestamp);
DECLARE default_filter numeric DEFAULT 0.05;
DECLARE min_requests int64 DEFAULT 5000;
DECLARE bid_rate_thr_1 float64 DEFAULT 75.0;
DECLARE bid_rate_thr_2 float64 DEFAULT 50.0;
DECLARE bid_rate_thr_3 float64 DEFAULT 25.0;
DECLARE bid_rate_thr_4 float64 DEFAULT 5.0;
DECLARE min_rpr float64 DEFAULT 0.001;

with all_country_entries as(
    SELECT bidder_id bidder, geo_continent continent,geo_country country,
           sum(ifnull(bids,0)) bids,
           sum(ifnull(bid_requests,0)) requests,
           sum(ifnull(estimated_adjusted_revenue,0)) rev,
           safe_divide(sum(ifnull(estimated_adjusted_revenue,0)),sum(ifnull(bid_requests,0)))*1000 rpr,
           safe_divide(sum(ifnull(bids,0)),sum(ifnull(bid_requests,0)))*100 bid_rate,
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
    CASE WHEN requests<=min_requests and country!='RU' THEN 'default' ELSE country END country,
    sum(ifnull(a.bids,0)) bids,
    sum(ifnull(a.bid_requests,0)) requests,
    sum(ifnull(a.estimated_adjusted_revenue,0)) rev,
    safe_divide(sum(ifnull(estimated_adjusted_revenue,0)),sum(ifnull(bid_requests,0)))*1000 rpr,
    safe_divide(sum(ifnull(a.bids,0)),sum(ifnull(a.bid_requests,0)))*100 bid_rate,
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
    sum(c.requests) over(partition by continent,country,bidder) requests,
    sum(c.rev) over(partition by continent,country,bidder) rev,
    avg(c.bid_rate) over(partition by continent,country,bidder) bid_rate,
    avg(c.rpr) over(partition by continent,country,bidder) rpr,
    avg(c.top_bid_rate) over(partition by continent,country,bidder) top_bid_rate,
    case when method='variable_threshold' then round((avg(top_bid_rate) over (partition by continent,country))*0.9,0) else 5 end thr
from country_entries_treated c
    left join threshold_type using (continent,country)
    ),

    country_level_results as(
select bidder,continent,country, range_,bidders_above_5,method,requests,rev,top_bid_rate,thr,bid_rate,rpr,
    case
    when country='RU' then 0
    when method is null then default_filter
    when (rev = 0.0 and rpr=0.0 and bid_rate=0.0 and rpr=0.0 and top_bid_rate is null) then default_filter
    when top_bid_rate<5 then default_filter
    when top_bid_rate<thr then default_filter
    else 1.0
    end filter
from countries_thr_cal
order by continent,country,top_bid_rate desc, bidder
    ),

    host_entries as(SELECT bidder_id bidder, geo_continent continent,geo_country country,host,
    sum(ifnull(bids,0)) bids,
    sum(ifnull(bid_requests,0)) requests,
    sum(ifnull(estimated_adjusted_revenue,0)) rev,
    sum(ifnull(case when error_code is null then total_top_bids end,0)) total_top_bids,
    safe_divide(sum(ifnull(estimated_adjusted_revenue,0)),sum(ifnull(bid_requests,0)))*1000 rpr,
    safe_divide(sum(ifnull(bids,0)),sum(ifnull(bid_requests,0)))*100 bid_rate,
    round(safe_divide(sum(ifnull(case when error_code is null then total_top_bids end,0)),sum(ifnull(case when error_code is null then bid_requests end,0)))*100,2) top_bid_rate,
FROM `freestar-prod.prebid_server_raw.unified_events_daily`
where (error_code IS NULL or error_code ='500')
  and record_date >= from_date AND record_date < to_date
  AND geo_continent IS NOT NULL
  AND geo_country IS NOT NULL
  AND bidder_id IS NOT NULL
group by 1,2,3,4
having sum(ifnull(bid_requests,0))>=min_requests
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
    sum(h.requests) over(partition by continent,country,host,bidder) requests,
    sum(h.rev) over(partition by continent,country,host,bidder) rev,
    avg(h.top_bid_rate) over(partition by continent,country,host,bidder) top_bid_rate,
    avg(h.bid_rate) over(partition by continent,country,host,bidder) bid_rate,
    avg(h.rpr) over(partition by continent,country,host,bidder) rpr,
    case when method='variable_threshold' then round((avg(top_bid_rate) over (partition by continent,country,host))*0.9,0) else 5 end thr
from host_entries h
    left join host_thr_type using (continent,country,host)
    ),

    host_level_results as(
select bidder,continent,country,host, range_,bidders_above_5,method,requests,rev,top_bid_rate,thr,bid_rate,rpr,
    case
    when country='RU' then 0
    when bidder='unrulyfsx' and bid_rate>=bid_rate_thr_4 and (rpr>=min_rpr) then 1
    when method is null then default_filter
    when top_bid_rate is null then default_filter
    when top_bid_rate<5 then default_filter
    when top_bid_rate<thr then default_filter

    else 1.0 end filter
from host_thr_cal
order by continent,country,top_bid_rate desc, bidder),

    unioned as (
select bidder,continent,country,'default' host,requests,rev,top_bid_rate,thr,bid_rate,rpr,filter
from country_level_results

union all

select bidder,continent,country,host,requests,rev,top_bid_rate,thr,bid_rate,rpr,filter
from host_level_results
    ),

    refinement as (SELECT *,
    case
    when filter=default_filter and (bid_rate>=bid_rate_thr_1) and (rpr>min_rpr) then 1
    when filter=default_filter and (bid_rate < bid_rate_thr_1 and bid_rate >= bid_rate_thr_2) and (rpr>min_rpr) then 0.75
    when filter=default_filter and (bid_rate < bid_rate_thr_2 and bid_rate >= bid_rate_thr_3) and (rpr>min_rpr) then 0.50
    when filter=default_filter and (bid_rate < bid_rate_thr_3 and bid_rate >= bid_rate_thr_4) and (rpr>min_rpr) then 0.25
    when filter=1 and (bid_rate < bid_rate_thr_4) and (rpr<min_rpr) and top_bid_rate<5.0 then default_filter
    when (filter=1 and rpr=0.0) then default_filter
    else filter end filter_adj
from unioned
    ),


    finalized as (select out.* from refinement out
    left join refinement in_ on in_.bidder=out.bidder and in_.continent=out.continent and in_.country=out.country and in_.host='default'
where out.host='default' or out.filter_adj!=in_.filter_adj
order by 1,2,3,4)


select bidder, continent,country,host, filter_adj filter from finalized