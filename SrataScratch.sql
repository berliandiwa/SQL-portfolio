-- Company = Amazon
-- Given a table of purchases by date, calculate the month-over-month percentage change in revenue. The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year.
-- The percentage change column will be populated from the 2nd month forward and can be calculated as ((this month's revenue - last month's revenue) / last month's revenue)*100.

SELECT DATE_FORMAT(created_at,'%Y-%m') AS ym,
       ROUND((SUM(value) - LAG(SUM(value)) OVER ())
             / LAG(SUM(value)) OVER ()
             * 100, 2) AS revenue_diff_pct
  FROM sf_transactions
 GROUP BY ym
 ORDER BY ym

-- Company = Microsoft
-- Find the total number of downloads for paying and non-paying users by date. Include only records where non-paying customers have more downloads than paying customers. The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads.

SELECT md.date,
    SUM(IF(ma.paying_customer = 'no',downloads, 0)) as non_paying,
      SUM(IF(ma.paying_customer = 'yes',downloads, 0)) as paying
FROM ms_user_dimension mu
LEFT JOIN ms_acc_dimension ma
    ON mu.acc_id = ma.acc_id
LEFT JOIN ms_download_facts md
    ON mu.user_id = md.user_id
GROUP BY md.date
HAVING non_paying>paying
ORDER BY md.date

-- Company = Google
-- Find the number of times the words 'bull' and 'bear' occur in the contents. We're counting the number of times the words occur so words like 'bullish' should not be included in our count.
-- Output the word 'bull' and 'bear' along with the corresponding number of occurrences

SELECT 'bull' AS word, 
       COUNT(CASE WHEN contents LIKE '%bull%' THEN 1 END) nentry
FROM google_file_store
WHERE contents NOT LIKE '%bullish%'
UNION ALL
SELECT 'bear' AS word, 
    COUNT(CASE WHEN contents LIKE '%bear%' THEN 1 END) nentry
from google_file_store
WHERE contents NOT LIKE '%bullish%'

-- Company = Airbnb
-- You’re given a table of rental property searches by users. The table consists of search results and outputs host information for searchers. Find the minimum, average, maximum rental prices for each host’s popularity rating. The host’s popularity rating is defined as below:
-- 0 reviews: New
-- 1 to 5 reviews: Rising
-- 6 to 15 reviews: Trending Up
-- 16 to 40 reviews: Popular
-- more than 40 reviews: Hot
-- Tip: The id column in the table refers to the search ID. You'll need to create your own host_id by concating price, room_type, host_since, zipcode, and number_of_reviews.
-- Output host popularity rating and their minimum, average and maximum rental prices.

WITH report1 AS 
(
SELECT concat(price,room_type,host_since,zipcode,number_of_reviews) host_id,
       price,
       number_of_reviews,
       CASE
            WHEN number_of_reviews = 0 THEN 'New'
            WHEN number_of_reviews BETWEEN 1 AND 5 THEN 'Rising'
            WHEN number_of_reviews BETWEEN 6 AND 15 THEN 'Trending Up'
            WHEN number_of_reviews BETWEEN 16 AND 40 THEN 'Popular'
            WHEN number_of_reviews > 40 THEN 'Hot'
            ELSE 'NULL'
        END AS category
from airbnb_host_searches
)
SELECT category, MIN(price), AVG(price), MAX(price)
FROM (SELECT DISTINCT host_id, price, category FROM report1) sc
GROUP BY 1

-- Company = Amazon
-- You have a table of in-app purchases by user. Users that make their first in-app purchase are placed in a marketing campaign where they see call-to-actions for more in-app purchases. Find the number of users that made additional in-app purchases due to the success of the marketing campaign.
-- The marketing campaign doesn't start until one day after the initial in-app purchase so users that only made one or multiple purchases on the first day do not count, nor do we count users that over time purchase only the products they purchased on the first day.

SELECT COUNT(DISTINCT user_id)
FROM 
(
     SELECT
        user_id, created_at, 
        DENSE_RANK() OVER(partition by user_id order by created_at) date_rnk,
        DENSE_RANK() OVER(partition by user_id, product_id order by created_at) product_rnk
    FROM marketing_campaign
)st
WHERE date_rnk > 1 AND product_rnk = 1
