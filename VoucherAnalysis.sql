-- Data Preparation
-- Data  Cleansing
SELECT DISTINCT currency_value FROM `bitlabs-dab.I_CID_03.order`;  -- nilai currency sudah sama semua, sehingga kolom currency_value tidak perlu di cleansing.  

SELECT COUNT(DISTINCT transaction_id) AS duplicate, COUNT(transaction_id) AS real_data 
FROM `bitlabs-dab.I_CID_03.order`; -- tidak ada duplicate data

/* Menghapus data yang mempunyai nilai voucher sebesar 0 dan tax_value sebesar 0 karena data tersebut dianggan data error. Akan tetapi, untuk voucher value yang bernilai 0 dan total value nya bernilai 4.3, tidak perlu dihapus karena dicurigai nilai tersebut tidak memenuhi minimal pembelian sehingga tidak mendappatkan voucher senilai 50%  */

WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),
-- Data Transformation --> Menambahkan kolom bulan, hari, ranking pembelian setiap customer
  df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT * FROM df3;

-- Eksploratory Data Analysis
-- 1. Melihat distribusi bulan data transaksi
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT year, month, COUNT(month) AS count_month 
  FROM df3
  GROUP BY year, month;
/* Berdasarkan hasil diatas, diketahui bahwa data transaksi terjadi pada bulan Maret dan Juni pada tahun 2022*/

-- 2. Melihat distirbusi voucher yang digunakan oleh customer flash coffee
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT voucher_name, COUNT(voucher_name) AS count_month 
  FROM df3
  GROUP BY 1;

WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT voucher_name, month, COUNT(voucher_name) AS count_month 
  FROM df3
  GROUP BY 1,2;

  /* Berdasarkan hasil diatas, diketahui bahwa : 
  1. pelanggan flash coffee lebih banyak untuk menggunakan voucher seniali 50% 
  2. Voucher senilai 50% digunakan pada bulan Maret dan Juni (hanya satu transaksi)
  3. Voucher senilai 25% hanya digunakan pada bulan Juni */

-- 3. Analisis dampak penggunaan voucher terhadap performance aplikasi Flash coffee
-- 3.1 Sales Revenue per Day
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT month, day, SUM(total_value) AS total_revenue, COUNT(DISTINCT transaction_id) AS number_of_transaction  
  FROM df3
  GROUP BY 1,2
  ORDER BY month ASC, day ASC;

-- 3.2 Repeat Customer Rate
-- Secara keseluruhan : 
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), repeatcust AS (
  SELECT COUNT(DISTINCT customer_id) AS num_of_repeat_cust
  FROM df3
  WHERE rank>1
), numcust AS (
  SELECT COUNT(DISTINCT customer_id) AS num_of_cust
  FROM df3
) 
  SELECT 
      repeatcust.num_of_repeat_cust, 
      numcust.num_of_cust, 
      (repeatcust.num_of_repeat_cust/numcust.num_of_cust)*100 AS repeat_cust_rate
  FROM repeatcust,numcust
;
-- Repeat customer rate berdasarkan jenis voucher
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), repeatt AS (
  SELECT voucher_name, COUNT(DISTINCT customer_id) AS num_repeat_cust 
  FROM df3
  WHERE rank>1
  GROUP BY 1
), cust AS(
  SELECT voucher_name, COUNT(DISTINCT customer_id) AS total_cust
  FROM df3
  GROUP BY 1
)
  SELECT
    repeatt.voucher_name,
    repeatt.num_repeat_cust,
    cust.total_cust,
    (repeatt.num_repeat_cust/cust.total_cust)*100 AS repeat_cust_rate
  FROM repeatt 
    LEFT JOIN cust 
      ON repeatt.voucher_name = cust.voucher_name;

-- Customer New Acquisition
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), min_transaction AS (
  SELECT customer_id, month, day
  FROM df3
  WHERE rank=1
)
  SELECT 
      month, 
      day, 
      COUNT(DISTINCT customer_id) AS cust_new_acquisition
  FROM min_transaction
  GROUP BY 1,2
  ORDER BY 1,2;

-- Customer Value Lifetime
-- Untuk mengetahui sebarapa besar nilai transaksi oleh setiap customer selama berlangganan pada flash coffee
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT customer_id, SUM(total_value) AS clv
  FROM df3
  GROUP BY 1
  ORDER BY 2 DESC;

  -- Average Order Value
  WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
  SELECT AVG(subTotal_value) AS avg_order
  FROM df3;

  -- Churn Rate
  -- Churn rate secara keseluruhan dengan menggunakan asumsi bahwa leave customer merupakan customer yang hanya bertransaksi satu kali.
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), trans AS(
  SELECT 
    customer_id,
    COUNT(DISTINCT transaction_id) AS num_of_trans
  FROM df3
  GROUP BY 1
), cust_leave AS(
  SELECT customer_id
  FROM trans
  WHERE num_of_trans = 1
)
SELECT COUNT(DISTINCT trans.customer_id) AS total_customer,
       COUNT(DISTINCT cust_leave.customer_id) AS num_of_leave_cust,
       (COUNT(DISTINCT cust_leave.customer_id)/COUNT(DISTINCT trans.customer_id))*100 AS churn_rate
FROM trans,cust_leave;

-- Churn rate berdasarkan jenis voucher
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), trans AS(
  SELECT
    voucher_name, 
    customer_id,
    COUNT(DISTINCT transaction_id) AS num_of_trans
  FROM df3
  GROUP BY 1,2
), cust_leave AS(
  SELECT voucher_name, COUNT(DISTINCT customer_id) AS num_cust_leave
  FROM trans
  WHERE num_of_trans = 1
  GROUP BY 1
)
SELECT trans.voucher_name,
       COUNT(DISTINCT trans.customer_id) AS num_of_trans,
       cust_leave.num_cust_leave AS num_cust_leave,        
       (cust_leave.num_cust_leave/ COUNT(DISTINCT trans.customer_id))*100 AS churn_rate
FROM trans 
LEFT JOIN cust_leave
    ON trans.voucher_name = cust_leave.voucher_name
GROUP BY 1,3;

-- Efektivitas Voucher
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
)
SELECT 
  voucher_name,
  COUNT(DISTINCT transaction_id) AS num_of_order,
  COUNT(DISTINCT customer_id) AS num_od_unique_cust,
  SUM(subTotal_value) AS gross_revenue,
  SUM(voucher_value) AS gross_discount_from_voucher,
  SUM(total_value) AS net_revenue,
  AVG(total_value) AS avg_net_order_value,
  AVG(voucher_value) AS avg_order_dicount
FROM df3
GROUP BY 1;

-- Retention cohort analysis
-- Cohort analysis untuk voucher 50%
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), first_cohort AS(
  SELECT 
      customer_id,
      MIN(transaction_date) AS first_cohort     
  FROM df3
  WHERE voucher_name='mass_voucher_50%'
  GROUP BY 1
), cohort AS(
  SELECT df3.customer_id,
         df3.transaction_date AS cohort,
         first_cohort.first_cohort,
         DATE_DIFF(df3.transaction_date, first_cohort.first_cohort, DAY) AS day_diff
  FROM df3
  LEFT JOIN first_cohort 
      ON df3.customer_id = first_cohort.customer_id
  WHERE df3.voucher_name = 'mass_voucher_50%'
), day0 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 0
  GROUP BY 1
), day1 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 1
  GROUP BY 1
), day2 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 2
  GROUP BY 1 
), day3 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 3
  GROUP BY 1
), day4 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 4
  GROUP BY 1
), day5 AS(
  SELECT first_cohort,
         COUNT(customer_id) AS cust
  FROM cohort
  WHERE day_diff = 5
  GROUP BY 1
), day6 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 6
  GROUP BY 1
), day7 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 7
  GROUP BY 1
), day8 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 8
  GROUP BY 1
), day9 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 9
  GROUP BY 1
), day10 AS(
  SELECT first_cohort,
         COUNT(customer_id) AS cust
  FROM cohort
  WHERE day_diff = 10
  GROUP BY 1
), day11 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 11
  GROUP BY 1
), day12 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 12
  GROUP BY 1
), day13 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 13
  GROUP BY 1
), day14 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 14
  GROUP BY 1
)
SELECT 
  day0.first_cohort, 
  CONCAT(ROUND((day0.cust/day0.cust)*100),'%') AS day_diff0, 
  CONCAT(ROUND((day1.cust/day0.cust)*100),'%') AS day_diff1,
  CONCAT(ROUND((day2.cust/day0.cust)*100),'%') AS day_diff2,
  CONCAT(ROUND((day3.cust/day0.cust)*100),'%') AS day_diff3,
  CONCAT(ROUND((day4.cust/day0.cust)*100),'%') AS day_diff4,
  CONCAT(ROUND((day5.cust/day0.cust)*100),'%') AS day_diff5,
  CONCAT(ROUND((day6.cust/day0.cust)*100),'%') AS day_diff6,
  CONCAT(ROUND((day7.cust/day0.cust)*100),'%') AS day_diff7,
  CONCAT(ROUND((day8.cust/day0.cust)*100),'%') AS day_diff8,
  CONCAT(ROUND((day9.cust/day0.cust)*100),'%') AS day_diff9,
  CONCAT(ROUND((day10.cust/day0.cust)*100),'%') AS day_diff10,
  CONCAT(ROUND((day11.cust/day0.cust)*100),'%') AS day_diff11,
  CONCAT(ROUND((day12.cust/day0.cust)*100),'%') AS day_diff12,
  CONCAT(ROUND((day13.cust/day0.cust)*100),'%') AS day_diff13,
  CONCAT(ROUND((day14.cust/day0.cust)*100),'%') AS day_diff14
FROM day0
LEFT JOIN day1
    ON day0.first_cohort = day1.first_cohort
LEFT JOIN day2
    ON day0.first_cohort = day2.first_cohort
LEFT JOIN day3
    ON day0.first_cohort = day3.first_cohort
LEFT JOIN day4
    ON day0.first_cohort = day4.first_cohort
LEFT JOIN day5
    ON day0.first_cohort = day5.first_cohort
LEFT JOIN day6
    ON day0.first_cohort = day6.first_cohort
LEFT JOIN day7
    ON day0.first_cohort = day7.first_cohort
LEFT JOIN day8
    ON day0.first_cohort = day8.first_cohort
LEFT JOIN day9
    ON day0.first_cohort = day9.first_cohort
LEFT JOIN day10
    ON day0.first_cohort = day10.first_cohort
LEFT JOIN day11
    ON day0.first_cohort = day11.first_cohort
LEFT JOIN day12
    ON day0.first_cohort = day12.first_cohort
LEFT JOIN day13
    ON day0.first_cohort = day13.first_cohort
LEFT JOIN day14
    ON day0.first_cohort = day14.first_cohort
ORDER BY 1;


-- Cohort analysis untuk voucher senilai 25%
WITH df AS(
  SELECT 
      *
  FROM `bitlabs-dab.I_CID_03.order`
  WHERE voucher_value != 0 OR subTotal_value = 4.3
), df2 AS(
  SELECT 
      * 
  FROM df 
  WHERE tax_value !=0
),df3 AS(
  SELECT
    *,
    EXTRACT (YEAR FROM transaction_time) AS year,
    EXTRACT (MONTH FROM transaction_time) AS month,
    EXTRACT (DAY FROM transaction_time) AS day,
    DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY transaction_time ASC) AS rank
  FROM df2
), first_cohort AS(
  SELECT 
      customer_id,
      MIN(transaction_date) AS first_cohort     
  FROM df3
  WHERE voucher_name='mass_voucher_25%'
  GROUP BY 1
), cohort AS(
  SELECT df3.customer_id,
         df3.transaction_date AS cohort,
         first_cohort.first_cohort,
         DATE_DIFF(df3.transaction_date, first_cohort.first_cohort, DAY) AS day_diff
  FROM df3
  LEFT JOIN first_cohort 
      ON df3.customer_id = first_cohort.customer_id
  WHERE df3.voucher_name = 'mass_voucher_25%'
), day0 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 0
  GROUP BY 1
), day1 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 1
  GROUP BY 1
), day2 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 2
  GROUP BY 1 
), day3 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 3
  GROUP BY 1
), day4 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 4
  GROUP BY 1
), day5 AS(
  SELECT first_cohort,
         COUNT(customer_id) AS cust
  FROM cohort
  WHERE day_diff = 5
  GROUP BY 1
), day6 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 6
  GROUP BY 1
), day7 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 7
  GROUP BY 1
), day8 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 8
  GROUP BY 1
), day9 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 9
  GROUP BY 1
), day10 AS(
  SELECT first_cohort,
         COUNT(customer_id) AS cust
  FROM cohort
  WHERE day_diff = 10
  GROUP BY 1
), day11 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 11
  GROUP BY 1
), day12 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 12
  GROUP BY 1
), day13 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 13
  GROUP BY 1
), day14 AS(
  SELECT first_cohort,
         COUNT(DISTINCT customer_id) AS cust
  FROM cohort
  WHERE day_diff = 14
  GROUP BY 1
)
SELECT 
  day0.first_cohort, 
  CONCAT(ROUND((day0.cust/day0.cust)*100),'%') AS day_diff0, 
  CONCAT(ROUND((day1.cust/day0.cust)*100),'%') AS day_diff1,
  CONCAT(ROUND((day2.cust/day0.cust)*100),'%') AS day_diff2,
  CONCAT(ROUND((day3.cust/day0.cust)*100),'%') AS day_diff3,
  CONCAT(ROUND((day4.cust/day0.cust)*100),'%') AS day_diff4,
  CONCAT(ROUND((day5.cust/day0.cust)*100),'%') AS day_diff5,
  CONCAT(ROUND((day6.cust/day0.cust)*100),'%') AS day_diff6,
  CONCAT(ROUND((day7.cust/day0.cust)*100),'%') AS day_diff7,
  CONCAT(ROUND((day8.cust/day0.cust)*100),'%') AS day_diff8,
  CONCAT(ROUND((day9.cust/day0.cust)*100),'%') AS day_diff9,
  CONCAT(ROUND((day10.cust/day0.cust)*100),'%') AS day_diff10,
  CONCAT(ROUND((day11.cust/day0.cust)*100),'%') AS day_diff11,
  CONCAT(ROUND((day12.cust/day0.cust)*100),'%') AS day_diff12,
  CONCAT(ROUND((day13.cust/day0.cust)*100),'%') AS day_diff13,
  CONCAT(ROUND((day14.cust/day0.cust)*100),'%') AS day_diff14
FROM day0
LEFT JOIN day1
    ON day0.first_cohort = day1.first_cohort
LEFT JOIN day2
    ON day0.first_cohort = day2.first_cohort
LEFT JOIN day3
    ON day0.first_cohort = day3.first_cohort
LEFT JOIN day4
    ON day0.first_cohort = day4.first_cohort
LEFT JOIN day5
    ON day0.first_cohort = day5.first_cohort
LEFT JOIN day6
    ON day0.first_cohort = day6.first_cohort
LEFT JOIN day7
    ON day0.first_cohort = day7.first_cohort
LEFT JOIN day8
    ON day0.first_cohort = day8.first_cohort
LEFT JOIN day9
    ON day0.first_cohort = day9.first_cohort
LEFT JOIN day10
    ON day0.first_cohort = day10.first_cohort
LEFT JOIN day11
    ON day0.first_cohort = day11.first_cohort
LEFT JOIN day12
    ON day0.first_cohort = day12.first_cohort
LEFT JOIN day13
    ON day0.first_cohort = day13.first_cohort
LEFT JOIN day14
    ON day0.first_cohort = day14.first_cohort
ORDER BY 1;
