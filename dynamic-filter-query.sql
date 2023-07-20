DECLARE fromDate timestamp DEFAULT CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS timestamp);
DECLARE toDate timestamp DEFAULT CAST(CURRENT_DATE() AS timestamp);
DECLARE defaultFilter numeric DEFAULT 0.05;
DECLARE minRequests int64 DEFAULT 5000;
DECLARE minBidRate numeric DEFAULT 0.25;
DECLARE minAverageBid numeric DEFAULT 0.2;
DECLARE minCombinedBidRate numeric DEFAULT 0.4;
DECLARE minCombinedAverageBid numeric DEFAULT 0.3;

WITH continentEntries AS (

    SELECT bidder_id Bidder, geo_continent Continent,
           Sum(bids) Bids,
           Sum(bids) + Sum(no_bids) Total,
           COALESCE(SAFE_DIVIDE(SUM(avg_adjusted_cpm*bids), SUM(bids)), 0) Average_Bid
    FROM `freestar-prod.prebid_server_raw.unified_events_daily`
    where error_code IS NULL
      AND geo_continent IS NOT NULL
      AND geo_continent != 'unknown'
    AND bidder_id IS NOT NULL
    AND record_date >= fromDate AND record_date < toDate
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
    Sum(bids) Bids,
    Sum(bids) + Sum(no_bids) Total,
    COALESCE(SAFE_DIVIDE(SUM(avg_adjusted_cpm*bids), SUM(bids)), 0) Average_Bid
FROM `freestar-prod.prebid_server_raw.unified_events_daily`
where error_code IS NULL
  AND geo_continent IS NOT NULL
  AND geo_country IS NOT NULL
  AND geo_continent != 'unknown'
  AND geo_country != 'unknown'
  AND bidder_id IS NOT NULL
  AND record_date >= fromDate AND record_date < toDate
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
   OR Country='RU'
    )
  AND Total > minRequests

    )

SELECT Bidder, Continent, Country, Region, Host, Filter
FROM (

         SELECT Bidder, Continent, null AS Country, null AS Region, null AS Host, Bids, Total, Average_Bid, defaultFilter AS Filter
         FROM continentEntries

         UNION ALL

         SELECT Bidder, Continent, 'RU' AS Country, null AS Region, null AS Host, Bids, Total, Average_Bid, 0 AS Filter
         FROM continentEntries WHERE Continent IN ('EU', 'AS')

         UNION ALL

         SELECT Bidder, Continent, null AS Country, null AS Region, Host, Bids, Total, Average_Bid, 1 AS Filter
         FROM
             (
                 SELECT bidder_id Bidder, geo_continent Continent, host AS Host,
                        Sum(bids) Bids,
                        Sum(bids) + Sum(no_bids) Total,
                        COALESCE(SAFE_DIVIDE(SUM(avg_adjusted_cpm*bids), SUM(bids)), 0) Average_Bid
                 FROM `freestar-prod.prebid_server_raw.unified_events_daily` AS auctions
                 where error_code IS NULL
                   AND geo_continent IS NOT NULL
                   AND geo_continent != 'unknown'
AND bidder_id IS NOT NULL
AND record_date >= fromDate AND record_date < toDate
AND geo_continent IN (SELECT Continent FROM continentEntries WHERE continentEntries.Bidder = auctions.bidder_id)
                 GROUP BY bidder_id, geo_continent, host
                 HAVING (
                     Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
                     OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
                     )
                    AND Total > minRequests
             )

         UNION ALL

         SELECT Bidder, Continent, Country, null AS Region, null AS Host, Bids, Total, Average_Bid,
                CASE WHEN Country='RU' THEN 0 ELSE defaultFilter END AS Filter
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
           AND NOT Country = 'RU'

         UNION ALL

         SELECT Bidder, Continent, Country, null AS Region, Host, Bids, Total, Average_Bid, 1 AS Filter
         FROM
             (
                 SELECT bidder_id Bidder, geo_continent Continent, geo_country Country, host AS Host,
                        Sum(bids) Bids,
                        Sum(bids) + Sum(no_bids) Total,
                        COALESCE(SAFE_DIVIDE(SUM(avg_adjusted_cpm*bids), SUM(bids)), 0) Average_Bid
                 FROM `freestar-prod.prebid_server_raw.unified_events_daily` AS auctions
                 where error_code IS NULL
                   AND geo_continent IS NOT NULL
                   AND geo_country IS NOT NULL
                   AND geo_continent != 'unknown'
AND geo_country != 'unknown'
AND bidder_id IS NOT NULL
AND record_date >= fromDate AND record_date < toDate
AND geo_country IN (SELECT Country FROM countryEntries WHERE countryEntries.Bidder = auctions.bidder_id)
                 GROUP BY bidder_id, geo_continent, geo_country, host
                 HAVING (
                     Bids/Total >= minCombinedBidRate OR Average_Bid >= minCombinedAverageBid
                     OR (Bids/Total >= minBidRate AND Average_Bid >= minAverageBid)
                     )
                    AND Total > minRequests
                    AND NOT geo_country = 'RU'
             )

     )
ORDER BY Bidder, Continent, Country, Region, Host