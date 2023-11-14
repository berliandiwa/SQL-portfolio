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

