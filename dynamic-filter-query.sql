DECLARE fromDate timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS timestamp);
DECLARE toDate timestamp DEFAULT CAST(CURRENT_DATE() AS timestamp);
DECLARE defaultFilter numeric DEFAULT 0.05;
DECLARE minRequests int64 DEFAULT 5000;
DECLARE minBidRate numeric DEFAULT 0.1;
DECLARE minAverageBid numeric DEFAULT 0.1;
DECLARE minCombinedBidRate numeric DEFAULT 0.3;
DECLARE minCombinedAverageBid numeric DEFAULT 0.2;


WITH continentEntries AS (

SELECT partner Bidder, geo_continent Continent,
Sum(bids) Bids,
Sum(bids) + Sum(no_bids) Total,
COALESCE(SAFE_DIVIDE(SUM(avg_bid_cpm*bids), SUM(bids)), 0) Average_Bid
FROM `streamamp-production.snapshots.pbs_a_daily`
where error_code IS NULL
AND geo_continent IS NOT NULL
AND geo_continent != 'unknown'
AND partner IS NOT NULL
AND time >= fromDate AND time < toDate
GROUP BY partner, geo_continent
HAVING (
    (Bids/Total < minCombinedBidRate AND Average_Bid < minCombinedAverageBid)
    OR Bids/Total < minBidRate
    OR Average_Bid < minAverageBid
)
AND Total > minRequests

),

allCountries AS (

SELECT partner Bidder, geo_continent Continent, geo_country Country,
Sum(bids) Bids,
Sum(bids) + Sum(no_bids) Total,
COALESCE(SAFE_DIVIDE(SUM(avg_bid_cpm*bids), SUM(bids)), 0) Average_Bid
FROM `streamamp-production.snapshots.pbs_a_daily`
where error_code IS NULL
AND geo_continent IS NOT NULL
AND geo_country IS NOT NULL
AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND partner IS NOT NULL
AND time >= fromDate AND time < toDate
GROUP BY partner, geo_continent, geo_country

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

)

SELECT Bidder, Continent, Country, Region, Host, Filter
FROM (

SELECT Bidder, Continent, null AS Country, null AS Region, null AS Host, Bids, Total, Average_Bid, defaultFilter AS Filter
FROM continentEntries

UNION ALL

SELECT Bidder, Continent, null AS Country, null AS Region, Host, Bids, Total, Average_Bid, 1 AS Filter
FROM
(
SELECT partner Bidder, geo_continent Continent, host AS Host,
Sum(bids) Bids,
Sum(bids) + Sum(no_bids) Total,
COALESCE(SAFE_DIVIDE(SUM(avg_bid_cpm*bids), SUM(bids)), 0) Average_Bid
FROM `streamamp-production.snapshots.pbs_a_daily` AS auctions
where error_code IS NULL
AND geo_continent IS NOT NULL
AND geo_continent != 'unknown'
AND partner IS NOT NULL
AND time >= fromDate AND time < toDate
AND geo_continent IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = auctions.partner)
GROUP BY partner, geo_continent, host
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
SELECT partner Bidder, geo_continent Continent, geo_country Country, host AS Host,
Sum(bids) Bids,
Sum(bids) + Sum(no_bids) Total,
COALESCE(SAFE_DIVIDE(SUM(avg_bid_cpm*bids), SUM(bids)), 0) Average_Bid
FROM `streamamp-production.snapshots.pbs_a_daily` AS auctions
where error_code IS NULL
AND geo_continent IS NOT NULL
AND geo_country IS NOT NULL
AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND partner IS NOT NULL
AND time >= fromDate AND time < toDate
AND geo_country IN (SELECT Country FROM countryEntries WHERE countryEntries.Bidder = auctions.partner)
GROUP BY partner, geo_continent, geo_country, host
HAVING (
    Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
    OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
)
AND Total > minRequests
)

)
ORDER BY Bidder, Continent, Country, Region, Host
