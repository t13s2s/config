DECLARE fromDate timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS timestamp);
DECLARE toDate timestamp DEFAULT CAST(CURRENT_DATE() AS timestamp);
DECLARE defaultFilter numeric DEFAULT 0.05;
DECLARE minRequests int64 DEFAULT 5000;

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
    (Bids/Total < 0.15 AND Average_Bid < 0.15)
    OR Bids/Total < 0.05
    OR Average_Bid < 0.05
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
    (Bids/Total < 0.15 AND Average_Bid < 0.15)
    OR Bids/Total < 0.05
    OR Average_Bid < 0.05
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
    (Bids/Total < 0.15 AND Average_Bid < 0.15)
    OR Bids/Total < 0.05
    OR Average_Bid < 0.05
)
AND Total > minRequests

)

SELECT Bidder, Continent, Country, Region, Filter
FROM (

SELECT Bidder, Continent, null AS Country, null AS Region, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM continentEntries

UNION ALL

SELECT Bidder, Continent, Country, null AS Region, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM countryEntries

UNION ALL 

SELECT Bidder, Continent, Country, null AS Region, Bids, Total, Average_Bid, 1 AS Filter
FROM allCountries  
WHERE Continent IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = allCountries.Bidder)
AND (
    Bids/Total >= 0.15 OR Average_Bid >= 0.15
    OR (Bids/Total >= 0.05 AND Average_Bid >= 0.05)
)
AND Total > minRequests

UNION ALL

SELECT Bidder, Continent, Country, Region, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM regionEntries

UNION ALL 

SELECT Bidder, Continent, Country, Region, Bids, Total, Average_Bid, 1 AS Filter
FROM allRegions 
WHERE Country IN (SELECT Country FROM countryEntries WHERE countryEntries.Bidder = allRegions.Bidder)
AND (
    Bids/Total >= 0.15 OR Average_Bid >= 0.15
    OR (Bids/Total >= 0.05 AND Average_Bid >= 0.05)
)
AND Total > minRequests

)
ORDER BY Bidder, Continent, Country, Region
