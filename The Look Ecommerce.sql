-- Find out what product category female and male customers purchase the most during 2022

-- Female Customers
WITH pelanggan_perempuan AS (
  SELECT 
    COUNT(oi.product_id) count_order, 
    u.gender, 
    p.category as category_p
  FROM `bitlabs-dab.thelook_ecommerce.order_items` oi
  INNER JOIN `bitlabs-dab.thelook_ecommerce.products` p
    ON oi.product_id = p.id
  INNER JOIN `bitlabs-dab.thelook_ecommerce.users` u
    ON oi.user_id = u.id
  WHERE u.gender = 'Female'
  GROUP BY u.gender, p.category
  ORDER BY count_order DESC
  LIMIT 1
),
-- Male Customers
pelanggan_laki AS (
  SELECT 
    COUNT(oi.product_id) count_order, 
    u.gender, 
    p.category as category_l
  FROM `bitlabs-dab.thelook_ecommerce.order_items` oi
  INNER JOIN `bitlabs-dab.thelook_ecommerce.products` p
    ON oi.product_id = p.id
  INNER JOIN `bitlabs-dab.thelook_ecommerce.users` u
    ON oi.user_id = u.id
  WHERE u.gender = 'Male'
  GROUP BY u.gender, p.category
  ORDER BY count_order DESC
  LIMIT 1
)
SELECT 
  pp.category_p AS Female, 
  lk.category_l AS Male 
FROM 
  pelanggan_perempuan pp, 
  pelanggan_laki lk;

/* Narasi
  Berdasarkan output yang dikeluarkan dari query tersebut, didapatkan bahwa kategori produk dengan pembelian paling banyak oleh pelanggan Laki-laki adalah jeans dengan jumlah order sebanyak 7644. Sedangkan untuk pelanggan perempuan, kategori produk dengan pembelian paling banyak adalah Intimates dengan jumlah order sebanyak 13607. 
  Analisis
Berdasarkan hasil query tersebut, pelanggan laki-laki paling tertarik dengan produk jeans, sedangkan pelanggan perempuan paling tertarik dengan produk intimates. Asumsi yang dapat disimpulkan yaitu : 
  - thelook paling sering mempromosikan produk jeans dan intimates kepada konsumen, baik dalam bentuk iklan,event, 
    atau diskon.  
  - Brand paling banyak yang ada di e-commerce thelook untuk perempuan adalah di kategori intimates, dan untuk     
    departemen laki-laki adalah di kategori jeans.
*/

-- Get the top 5 least and most profitable product over all time
-- The Top 5 most profitable product over all time
SELECT 
  COUNT(oi.product_id) count_order,
  oi.product_id,
  p.name name_product, 
  p.category,
  SUM((oi.sale_price-p.cost)) as sum_profit 
FROM `bitlabs-dab.thelook_ecommerce.order_items` oi
INNER JOIN `bitlabs-dab.thelook_ecommerce.products` p
  ON oi.product_id = p.id
GROUP BY oi.product_id, p.name, p.category
ORDER BY sum_profit DESC
LIMIT 5;

-- The Top 5 least profitable product over all time
SELECT 
  COUNT(oi.product_id) count_order,
  oi.product_id, 
  p.name name_product,
  p.category, 
  SUM((oi.sale_price-p.cost)) as sum_profit
FROM `bitlabs-dab.thelook_ecommerce.order_items` oi
INNER JOIN `bitlabs-dab.thelook_ecommerce.products` p
  ON oi.product_id = p.id
GROUP BY oi.product_id, p.name, p.category
ORDER BY sum_profit ASC
LIMIT 5;

/* Analisis
1. Produk yang paling profitable adalah Nobis Yatesy Parka dengan jumlah profit 6817,199... 
2. Produk yang paling tidak profitable adalah Indestructable aluminium dengan jumlah profit sebasar 0,035...
3. Banyaknya order tidak menjamin besarnya profit
4. Product yang paling profitable banyak berasal dari kategori outerwear & coats
5. Produk yang paling tidak profitable banyak berasal dari kategori accessoris
*/

-- Find out how much the company are selling per day, how many orders per day and how many customers purchased per day
WITH report1 AS (
  SELECT 
    FORMAT_DATETIME('%Y-%m-%d',created_at) date_time, 
    SUM(sale_price) AS sum_sales,  
    COUNT(DISTINCT(order_id)) AS count_order, 
    COUNT(DISTINCT(user_id)) AS count_user
  FROM `bitlabs-dab.thelook_ecommerce.order_items`
  GROUP BY date_time
  ORDER BY date_time
) 
SELECT 
  AVG(report1.sum_sales) sum_sales, 
  AVG(report1.count_order) count_order, 
  AVG(report1.count_user) count_user
FROM report1;


/* Analisis
1. Jumlah penjualan, jumlah order, dan user mengalami peningkatan setiap tahunnya
2. Jika dilihat dari jumlah perharinya, jumlah penjualan, jumlah order, dan jumlah user mengalami kenaikan yang signifikan (hampir 50% dan mencapai 1000 order) pada 5 hari (14-05-2022 sampai 18-05-2022) di tahun 2022 dan mengalami penurunan kembali pada hari selanjutnya. Setelah dianalisis dari tabel events, banyak user yang juga melakukan aktivitas pada tanggal tersebut.
3. Jika dirata-rata, jumlah penjualan thelook tiap harinya sebesar 8797, jumlah ordernya sebesar 136 order, dan jumlah user sebesar 135 user. 
*/

-- Find monthly growth of inventory in percentage breakdown by product categories from Jan 2019 until Jan 2022 (*growth = (current profit - prev profit) / prev profit)
WITH report1 AS(
  SELECT
    FORMAT_DATETIME('%Y-%m',oi.created_at) ym, 
    p.category category,
    SUM(oi.sale_price-p.cost) profit
  FROM `bitlabs-dab.thelook_ecommerce.order_items`oi
  JOIN `bitlabs-dab.thelook_ecommerce.products` p
    ON oi.product_id = p.id
  WHERE 
    oi.created_at BETWEEN '2019-01-01' AND '2022-02-01' AND
    oi.status = 'Complete'
  GROUP BY ym, category
  ORDER BY ym
)
SELECT 
  report1.ym year_month,
  report1.category category,
  report1.profit,
  LAG(report1.profit) OVER (PARTITION BY report1.category ORDER BY report1.ym) profit_sebelumnya,
  CONCAT(ROUND((report1.profit - LAG(report1.profit) OVER (PARTITION BY report1.category ORDER BY report1.ym))
        /LAG(report1.profit) OVER (PARTITION BY report1.category ORDER BY report1.ym)*100,2),'%') AS monthly_growth
FROM report1
ORDER BY category;


/*Analisis 
1. Monthly growth terbesar terjadi pada awal tahun 2019, yaitu pada awal pendirian.Hal ini berarti penjualan pada tahun berikutnya tidak terlalu banyak mengalami perubahan profit pada setiap kategori. 
2. Setelah dianalisis dengan bantuan google sheet, kategori produk dengan rata-rata monthly growth tertinggi sepanjang tahun 2019 sampai 2022 adalah Jumpsuit & rompers. Sedangkan rata-rata monthly growth terendah adalah kategori outerwear & coats
4. Banyak kategori produk yang mengalami penurunan pertumbuhan (minus) pada tahun 2021 
5. Hasil ini bisa digunakan untuk menentukan penempatan promosi yang tepat (sesuai kategori)
*/

-- Get frequencies, average order value and total number of unique users where status is complete grouped by month from Jan 2019 until Jan 2022
WITH report1 AS 
(
    SELECT 
      FORMAT_DATETIME('%Y-%m',oe.created_at) ym, 
      oe.order_id order_id, 
      SUM(oi.sale_price) sum_sale,
      oe.user_id user
    FROM `bitlabs-dab.thelook_ecommerce.orders` oe 
    LEFT JOIN `bitlabs-dab.thelook_ecommerce.order_items` oi
      ON oe.order_id = oi.order_id
    WHERE 
      oe.status = 'Complete' AND 
      oe.created_at BETWEEN '2019-01-01' AND '2022-02-01'
    GROUP BY ym, order_id, user
    ORDER BY ym, order_id
)
SELECT 
  report1.ym year_month, 
  COUNT(distinct(report1.order_id)) count_order, 
  AVG(report1.sum_sale) AOV, 
  COUNT(DISTINCT(report1.user)) count_user 
FROM report1
GROUP BY report1.ym
ORDER BY report1.ym ASC
/*
Analisis : 
1. Setiap bulannya, satu user melakukan lebih dari satu transkasi 
2. Setiap tahun, frekuensi dan value order selalu mengalami peningkatan
3. Frekuensi, aov, dan jumlah user selalu mengalami pengingkatan pada akhir tahun
4. Setiap tahun, user yang melakukan repeat order mengalami peningkatan
5. Hasil ini dapat digunakan untuk membantu dalam memilih strategi pemilihan harga dan strategi promosi yang tepat. 
*/
