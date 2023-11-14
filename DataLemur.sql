-- Company = Alibaba
-- You're given a table containing the item count for each order on Alibaba, along with the frequency of orders that have the same item count. Write a query to retrieve the mode of the order occurrences. Additionally, if there are multiple item counts with the same mode, the results should be sorted in ascending order.
WITH df AS(
  SELECT 
    item_count, 
    order_occurrences,
    RANK() OVER(ORDER BY order_occurrences DESC) ranking
  FROM items_per_order
)
SELECT item_count AS mode
FROM df
WHERE ranking = 1
ORDER BY 1 

-- Company = JPMorgan Chase 
-- Your team at JPMorgan Chase is soon launching a new credit card. You are asked to estimate how many cards you'll issue in the first month.
-- Before you can answer this question, you want to first get some perspective on how well new credit card launches typically do in their first month.
-- Write a query that outputs the name of the credit card, and how many cards were issued in its launch month. The launch month is the earliest record in the monthly_cards_issued table for a given card. Order the results starting from the biggest issued amount.
WITH df AS(
SELECT
   *,
   RANK() OVER(PARTITION BY card_name ORDER BY issue_year, issue_month ASC)
FROM monthly_cards_issued
)
SELECT card_name, issued_amount
FROM df
WHERE rank=1
ORDER BY issued_amount DESC

-- Company = Tiktok
-- New TikTok users sign up with their emails. They confirmed their signup by replying to the text confirmation to activate their accounts. Users may receive multiple text messages for account confirmation until they have confirmed their new account.
-- A senior analyst is interested to know the activation rate of specified users in the emails table. Write a query to find the activation rate. Round the percentage to 2 decimal places.

WITH df AS(
  SELECT em.email_id, em.user_id, tx.text_id, tx.signup_action
  FROM emails em 
  LEFT JOIN texts tx
    ON em.email_id = tx.email_id
) 
SELECT 
  ROUND(COUNT(DISTINCT user_id) FILTER (WHERE signup_action = 'Confirmed') :: DECIMAL
  /COUNT (DISTINCT user_id),2) AS activation_rate
FROM df
