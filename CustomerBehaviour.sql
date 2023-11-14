-- Data Preparation
-- Menggabungkan kedua tabel
SELECT *
FROM `bitlabs-dab.I_CID_02.activity` a
LEFT JOIN `bitlabs-dab.I_CID_02.user` u
      ON a.masked_user_id = u.masked_user_id;

-- Data Cleansing
-- Cek missing value pada tabel activity 
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
), cek_df AS (
      SELECT * 
      FROM df
      WHERE masked_user_id IS NULL OR 
            calendar_date IS NULL OR
            view_pdp_count IS NULL OR
            search_count IS NULL OR
            view_microsite_count IS NULL OR
            session_start_global_count IS NULL OR
            wishlist_count IS NULL OR
            pilih_metode_pembayaran_count IS NULL OR
            input_kode_promo_count IS NULL OR
            is_install IS NULL OR
            os IS NULL OR
            city IS NULL OR
            region IS NULL 
)
 SELECT COUNT(masked_user_id) FROM cek_df;

-- Handling missing value
-- Karena yang berisi null kurang dari 5%, maka data tersebut akan dihapus

WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
)
 SELECT COUNT(masked_user_id) 
 FROM df2 
 WHERE city IS NULL OR region IS NULL;

-- Menambahkan kolom minggu dan tipe device
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), all_visit AS (
-- Exploratory Data Analysis
-- Conversion Rate Per Page
 SELECT COUNT(DISTINCT masked_user_id) AS visitors 
 FROM df5
), session_visit AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE session_start_global_count>0
), conversion_session AS (
   SELECT 'start_session' AS page, b.visitors_step,a.visitors, CONCAT(ROUND((b.visitors_step/a.visitors)*100),'%' )AS conversion_rate
   FROM all_visit a, session_visit b
), search_visit AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE search_count>0
), conversion_search AS (
   SELECT 'search' AS page, a.visitors_step, b.visitors, CONCAT(ROUND((a.visitors_step/b.visitors)*100),'%' ) conversion_rate
   FROM search_visit a, all_visit b
), microsite_visit AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE view_microsite_count>0
), conversion_microsite AS (
   SELECT 'microsite' AS page, c.visitors_step, b.visitors, CONCAT(ROUND((c.visitors_step/b.visitors)*100),'%' ) conversion_rate
   FROM microsite_visit c, all_visit b
), pdp_visit AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE view_pdp_count>0
), conversion_pdp AS (
   SELECT 'pdp' AS page, d.visitors_step, b.visitors, CONCAT(ROUND((d.visitors_step/b.visitors)*100),'%' )
   FROM pdp_visit d, all_visit b
), wishlist_visit AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE wishlist_count>0
), conversion_wishlist AS (
   SELECT 'wishlist' AS page, e.visitors_step, b.visitors, CONCAT(ROUND((e.visitors_step/b.visitors)*100),'%' ) conversion_rate
   FROM wishlist_visit e, all_visit b
), promo_visit AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE input_kode_promo_count>0
), conversion_promo AS (
   SELECT 'input_promo' AS page, f.visitors_step, b.visitors, CONCAT(ROUND((f.visitors_step/b.visitors)*100),'%' ) conversion_rate
   FROM promo_visit f, all_visit b
), payment_visit AS(
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE pilih_metode_pembayaran_count>0
), conversion_payment AS(
   SELECT 'payment' AS page, g.visitors_step, b.visitors, CONCAT(ROUND((g.visitors_step/b.visitors)*100),'%' ) conversion_rate
   FROM payment_visit g, all_visit b
), purchase AS (
   SELECT COUNT(DISTINCT masked_user_id) visitors_step
   FROM df5 
   WHERE is_make_order=True
), conversion_purchase AS(
   SELECT 'purchase' AS page, h.visitors_step, b.visitors, CONCAT(ROUND((h.visitors_step/b.visitors)*100),'%' ) conversion_rate
   FROM purchase h, all_visit b
)
 SELECT *
 FROM conversion_session
 UNION ALL
 SELECT *
 FROM conversion_search
 UNION ALL
 SELECT *
 FROM conversion_microsite
 UNION ALL
 SELECT *
 FROM conversion_pdp
 UNION ALL
 SELECT *
 FROM conversion_wishlist
 UNION ALL
 SELECT *
 FROM conversion_promo
 UNION ALL
 SELECT *
 FROM conversion_payment
 UNION ALL
 SELECT *
 FROM conversion_purchase;

-- Conversion rate per stage
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
/* Stage 1 : memulai sessionn ke melihat detail produk
   Stage 2 : melihat detail produk ke memilih metode pembayaran
   Stage 3 : memilih metode pembayaran ke melakukan transaski */
), step1 AS(
   SELECT 'step0' step,COUNT(DISTINCT masked_user_id) visitor_per_step
   FROM df5 
   WHERE session_start_global_count>0
), step2 AS(
   SELECT 'step1' step,COUNT(DISTINCT masked_user_id) visitor_per_step
   FROM df5
   WHERE view_pdp_count > 0 
), step3 AS(
   SELECT 'step2' step, COUNT(DISTINCT masked_user_id) visitor_per_step
   FROM df5
   WHERE pilih_metode_pembayaran_count>0
), step4 AS(
   SELECT 'step3' step,COUNT(DISTINCT masked_user_id) visitor_per_step
   FROM df5
   WHERE is_make_order=True
), union_step AS(
   SELECT * FROM step1
   UNION ALL
   SELECT * FROM step2
   UNION ALL
   SELECT * FROM step3 
   UNION ALL
   SELECT * FROM step4 
   ORDER BY step  
)
  SELECT *,
         CONCAT((visitor_per_step/LAG(visitor_per_step) OVER (ORDER BY step)*100),'%') conversion_rate
  FROM union_step
  ORDER BY step;

--   Conversion rate per device type
-- Conversion : yang melakukan transaksi pembelian
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
),web AS(
   SELECT COUNT(DISTINCT masked_user_id) visitors_device
   FROM df5
   WHERE type_device='Web'
),web_conversion AS(
   SELECT COUNT(DISTINCT masked_user_id) conversion_device
   FROM df5
   WHERE type_device = 'Web' AND is_make_order = True
), conversion_rate_web AS(
   SELECT 'Web' type_device,a.visitors_device, b.conversion_device, CONCAT(ROUND((b.conversion_device/a.visitors_device)*100),'%') conversion_rate
   FROM web a, web_conversion b
), mobile AS(
   SELECT COUNT(DISTINCT masked_user_id) visitors_device
   FROM df5
   WHERE type_device='Mobile'
),mobile_conversion AS(
   SELECT COUNT(DISTINCT masked_user_id) conversion_device
   FROM df5
   WHERE type_device = 'Mobile' AND is_make_order = True
), conversion_rate_mobile AS(
   SELECT 'Mobile' type_device, a.visitors_device, b.conversion_device, CONCAT(ROUND((b.conversion_device/a.visitors_device)*100),'%') conversion_rate
   FROM mobile a, mobile_conversion b
), app AS(
   SELECT COUNT(DISTINCT masked_user_id) visitors_device
   FROM df5
   WHERE type_device='App'
),app_conversion AS(
   SELECT COUNT(DISTINCT masked_user_id) conversion_device
   FROM df5
   WHERE type_device = 'App' AND is_make_order = True
), conversion_rate_app AS(
   SELECT 'App' type_device,a.visitors_device, b.conversion_device, CONCAT(ROUND((b.conversion_device/a.visitors_device)*100),'%') conversion_rate
   FROM app a, app_conversion b
)
 SELECT * FROM conversion_rate_web
 UNION ALL
 SELECT * FROM conversion_rate_mobile
 UNION ALL
 SELECT * FROM conversion_rate_app;

--  Complete Customers
-- Complete customers berdasarkan jenis device
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), complete_cust AS(
   SELECT *
   FROM df5
   WHERE session_start_global_count>0 AND view_pdp_count>0 AND pilih_metode_pembayaran_count>0 AND is_make_order = True
)
SELECT type_device, COUNT(DISTINCT masked_user_id) AS complete_cust_count
FROM complete_cust
GROUP BY type_device;

-- Complete Cust Berdasarkan nama region
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), complete_cust AS(
   SELECT *
   FROM df5
   WHERE session_start_global_count>0 AND view_pdp_count>0 AND pilih_metode_pembayaran_count>0 AND is_make_order = True
)
SELECT region, COUNT(DISTINCT masked_user_id) AS complete_cust_count
FROM complete_cust
GROUP BY 1
ORDER BY 2 DESC;

-- Bounce Rate pada setiap page
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
),visit_session AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE session_start_global_count>0
), bounce_session AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count>0 AND search_count=0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_session AS(
   SELECT 'session' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_session a, bounce_session b
),visit_search AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE search_count>0
), bounce_search AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count=0 AND search_count>0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_search AS(
   SELECT 'search' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_search a, bounce_search b
),visit_microsite AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE view_microsite_count>0
), bounce_microsite AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count=0 AND search_count=0 AND view_microsite_count>0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_microsite AS(
   SELECT 'microsite' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_microsite a, bounce_microsite b
),visit_pdp AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE view_pdp_count>0
), bounce_pdp AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count=0 AND search_count=0 AND view_microsite_count=0 AND view_pdp_count>0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_pdp AS(
   SELECT 'pdp' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_pdp a, bounce_pdp b
),visit_wishlist AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE wishlist_count>0
), bounce_wishlist AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count>0 AND search_count=0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count > 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_wishlist AS(
   SELECT 'wishlist' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_wishlist a, bounce_wishlist b
), visit_promo AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE input_kode_promo_count>0
), bounce_promo AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count>0 AND search_count=0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count>0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_promo AS(
   SELECT 'Input Promo' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_promo a, bounce_promo b
),visit_payment AS(
   SELECT COUNT(DISTINCT masked_user_id) visit_per_page
   FROM df5
   WHERE pilih_metode_pembayaran_count>0
), bounce_payment AS(
   SELECT COUNT(DISTINCT masked_user_id) bounce_page
   FROM df5
   WHERE session_start_global_count=0 AND search_count=0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count>0 AND is_make_order = False
), bounce_rate_payment AS(
   SELECT 'Payment' AS page, a.visit_per_page, b.bounce_page, CONCAT(ROUND((b.bounce_page/a.visit_per_page)*100),'%') bounce_rate
   FROM visit_payment a, bounce_payment b
) 
 SELECT * FROM bounce_rate_session 
 UNION ALL
 SELECT * FROM bounce_rate_search
 UNION ALL
 SELECT * FROM bounce_rate_microsite
 UNION ALL
 SELECT * FROM bounce_rate_pdp
 UNION ALL
 SELECT * FROM bounce_rate_wishlist
 UNION ALL
 SELECT * FROM bounce_rate_promo
 UNION ALL
 SELECT * FROM bounce_rate_payment;

-- Exit Rate 
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), visit_session AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE session_start_global_count>0
), exit_session AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE session_start_global_count>0 AND search_count=0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_session AS (
      SELECT 'session' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_session a, exit_session b
), visit_search AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE search_count>0
), exit_search AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE session_start_global_count>=0 AND search_count>0 AND view_microsite_count=0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False
), bounce_rate_search AS (
      SELECT 'search' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_search a, exit_search b
), visit_microsite AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE view_microsite_count>0
), exit_microsite AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE (session_start_global_count>=0 OR search_count>=0) AND (view_microsite_count>0 AND view_pdp_count=0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False)
), bounce_rate_microsite AS (
      SELECT 'microsite' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_microsite a, exit_microsite b
), visit_pdp AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE view_pdp_count>0
), exit_pdp AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE (session_start_global_count>=0 OR search_count>=0 OR view_microsite_count>=0) AND (view_pdp_count>0 AND wishlist_count = 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False)
), bounce_rate_pdp AS (
      SELECT 'view_pdp' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_pdp a, exit_pdp b
), visit_wishlist AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE wishlist_count>0
), exit_wishlist AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE (session_start_global_count>=0 OR search_count>=0 OR view_microsite_count>=0 OR view_pdp_count>=0) AND (wishlist_count > 0 AND input_kode_promo_count=0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False)
), bounce_rate_wishlist AS (
      SELECT 'wishlist' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_wishlist a, exit_wishlist b
), visit_promo AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE input_kode_promo_count>0
), exit_promo AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE (session_start_global_count>=0 OR search_count>=0 OR view_microsite_count>=0 OR view_pdp_count>=0 OR wishlist_count >= 0) AND (input_kode_promo_count>0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False)
), bounce_rate_promo AS (
      SELECT 'input_promo' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_promo a, exit_promo b
), visit_payment AS (
      SELECT COUNT(masked_user_id) visitor_page
      FROM df5
      WHERE pilih_metode_pembayaran_count>0
), exit_payment AS(
      SELECT COUNT(masked_user_id) exit_page
      FROM df5
      WHERE (session_start_global_count>=0 OR search_count>=0 OR view_microsite_count>=0 OR view_pdp_count>=0 OR wishlist_count >= 0 OR input_kode_promo_count>=0) AND (pilih_metode_pembayaran_count>0 AND is_make_order = False)
), bounce_rate_payment AS (
      SELECT 'payment' AS page, a.visitor_page, b.exit_page, CONCAT(ROUND((b.exit_page/a.visitor_page)*100),'%') bounce_rate
      FROM visit_payment a, exit_payment b
) 
 SELECT * FROM bounce_rate_session
 UNION ALL
 SELECT * FROM bounce_rate_search
 UNION ALL
 SELECT * FROM bounce_rate_microsite
 UNION ALL
 SELECT * FROM bounce_rate_pdp
 UNION ALL
 SELECT * FROM bounce_rate_wishlist
 UNION ALL
 SELECT * FROM bounce_rate_promo
 UNION ALL
 SELECT * FROM bounce_rate_payment
 ORDER BY 4 ASC;

-- User Behavior
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
)
 SELECT event_type, COUNT(DISTINCT masked_user_id) customer_count
 FROM df5
 GROUP BY 1;

--  User Behaviour per Week
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), week1 AS(
   SELECT event_type, COUNT(DISTINCT masked_user_id) customer_count
   FROM df5
   WHERE week = 1
   GROUP BY 1
), week2 AS(
   SELECT event_type, COUNT(DISTINCT masked_user_id) customer_count
   FROM df5
   WHERE week = 2
   GROUP BY 1
),week3 AS(
   SELECT event_type, COUNT(DISTINCT masked_user_id) customer_count
   FROM df5
   WHERE week = 3
   GROUP BY 1
), week4 AS(
   SELECT event_type, COUNT(DISTINCT masked_user_id) customer_count
   FROM df5
   WHERE week = 4
   GROUP BY 1
), week5 AS(
   SELECT event_type, COUNT(DISTINCT masked_user_id) customer_count
   FROM df5
   WHERE week = 5
   GROUP BY 1
) 
 SELECT week1.event_type,
        week1.customer_count week1,
        week2.customer_count week2, 
        week3.customer_count week3,
        week4.customer_count week4,
        week5.customer_count week5
FROM week1 
LEFT JOIN week2 
   ON week1.event_type = week2.event_type
LEFT JOIN week3
   ON week1.event_type = week3.event_type
LEFT JOIN week4 
   ON week1.event_type = week4.event_type
LEFT JOIN week5 
   ON week1.event_type = week5.event_type
ORDER BY 2 DESC;

-- Page Views
-- Page Views Secara Keseluruhan
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), session AS(
   SELECT 'Session' AS page, SUM(session_start_global_count) pageviews
   FROM df5
), searchh AS(
   SELECT 'Search' AS page, SUM(search_count) pageviews
   FROM df5
), microsite AS (
   SELECT 'Microsite' AS page, SUM(view_microsite_count) pageviews
   FROM df5
), pdp AS(
     SELECT 'pdp' AS page, SUM(view_pdp_count) pageviews
   FROM df5
), wishlist AS (
   SELECT 'Wishlist' AS page, SUM(wishlist_count) pageviews
   FROM df5
), promo AS (
   SELECT 'Input Promo' AS page, SUM(input_kode_promo_count) pageviews
   FROM df5
), payment AS(
   SELECT 'Payment' AS page, SUM(pilih_metode_pembayaran_count) pageviews
   FROM df5
)
 SELECT * FROM session
 UNION ALL
 SELECT * FROM searchh
 UNION ALL
 SELECT * FROM microsite
 UNION ALL
 SELECT * FROM pdp
 UNION ALL
 SELECT * FROM wishlist
 UNION ALL
 SELECT * FROM promo
 UNION ALL
 SELECT * FROM payment
 ORDER BY 2 DESC;

-- Page Views Per Week
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), week1_search AS(
      SELECT SUM(search_count) week1
      FROM df5
      WHERE week=1
), week2_search AS(
      SELECT SUM(search_count) week2
      FROM df5
      WHERE week=2
), week3_search AS(
      SELECT SUM(search_count) week3
      FROM df5
      WHERE week=3
), week4_search AS (
      SELECT SUM(search_count) week4
      FROM df5
      WHERE week=4
), week5_search AS(
      SELECT SUM(search_count) week5
      FROM df5
      WHERE week=5
), search_pv AS(
      SELECT 'Search' as page, a.week1, b.week2, c.week3, d.week4, e.week5
      FROM week1_search a, week2_search b, week3_search c, week4_search d, week5_search e
), week1_microsite AS(
      SELECT SUM(view_microsite_count) week1
      FROM df5
      WHERE week=1
), week2_microsite AS(
      SELECT SUM(view_microsite_count) week2
      FROM df5
      WHERE week=2
), week3_microsite AS(
      SELECT SUM(view_microsite_count) week3
      FROM df5
      WHERE week=3
), week4_microsite AS (
      SELECT SUM(view_microsite_count) week4
      FROM df5
      WHERE week=4
), week5_microsite AS(
      SELECT SUM(view_microsite_count) week5
      FROM df5
      WHERE week=5
), microsite_pv AS(
      SELECT 'Microsite' as page, a.week1, b.week2, c.week3, d.week4, e.week5
      FROM week1_microsite a, week2_microsite b, week3_microsite c, week4_microsite d, week5_microsite e
),week1_pdp AS(
      SELECT SUM(view_pdp_count) week1
      FROM df5
      WHERE week=1
), week2_pdp AS(
      SELECT SUM(view_pdp_count) week2
      FROM df5
      WHERE week=2
), week3_pdp AS(
      SELECT SUM(view_pdp_count) week3
      FROM df5
      WHERE week=3
), week4_pdp AS (
      SELECT SUM(view_pdp_count) week4
      FROM df5
      WHERE week=4
), week5_pdp AS(
      SELECT SUM(view_pdp_count) week5
      FROM df5
      WHERE week=5
), pdp_pv AS(
      SELECT 'PDP' as page, a.week1, b.week2, c.week3, d.week4, e.week5
      FROM week1_pdp a, week2_pdp b, week3_pdp c, week4_pdp d, week5_pdp e
), week1_wishlist AS(
      SELECT SUM(wishlist_count) week1
      FROM df5
      WHERE week=1
), week2_wishlist AS(
      SELECT SUM(wishlist_count) week2
      FROM df5
      WHERE week=2
), week3_wishlist AS(
      SELECT SUM(wishlist_count) week3
      FROM df5
      WHERE week=3
), week4_wishlist AS (
      SELECT SUM(wishlist_count) week4
      FROM df5
      WHERE week=4
), week5_wishlist AS(
      SELECT SUM(wishlist_count) week5
      FROM df5
      WHERE week=5
), wishlist_pv AS(
      SELECT 'Wishlist' as page, a.week1, b.week2, c.week3, d.week4, e.week5
      FROM week1_wishlist a, week2_wishlist b, week3_wishlist c, week4_wishlist d, week5_wishlist e
), week1_promo AS(
      SELECT SUM(input_kode_promo_count) week1
      FROM df5
      WHERE week=1
), week2_promo AS(
      SELECT SUM(input_kode_promo_count) week2
      FROM df5
      WHERE week=2
), week3_promo AS(
      SELECT SUM(input_kode_promo_count) week3
      FROM df5
      WHERE week=3
), week4_promo AS (
      SELECT SUM(input_kode_promo_count) week4
      FROM df5
      WHERE week=4
), week5_promo AS(
      SELECT SUM(input_kode_promo_count) week5
      FROM df5
      WHERE week=5
), promo_pv AS(
      SELECT 'Promo' as page, a.week1, b.week2, c.week3, d.week4, e.week5
      FROM week1_promo a, week2_promo b, week3_promo c, week4_promo d, week5_promo e
), week1_payment AS(
      SELECT SUM(pilih_metode_pembayaran_count) week1
      FROM df5
      WHERE week=1
), week2_payment AS(
      SELECT SUM(pilih_metode_pembayaran_count) week2
      FROM df5
      WHERE week=2
), week3_payment AS(
      SELECT SUM(pilih_metode_pembayaran_count) week3
      FROM df5
      WHERE week=3
), week4_payment AS (
      SELECT SUM(pilih_metode_pembayaran_count) week4
      FROM df5
      WHERE week=4
), week5_payment AS(
      SELECT SUM(pilih_metode_pembayaran_count) week5
      FROM df5
      WHERE week=5
), payment_pv AS(
      SELECT 'Payment' as page, a.week1, b.week2, c.week3, d.week4, e.week5
      FROM week1_payment a, week2_promo b, week3_promo c, week4_promo d, week5_promo e
)
 SELECT * FROM search_pv 
 UNION ALL 
 SELECT * FROM microsite_pv 
 UNION ALL 
 SELECT * FROM pdp_pv 
 UNION ALL 
 SELECT * FROM wishlist_pv 
 UNION ALL 
 SELECT * FROM promo_pv 
 UNION ALL 
 SELECT * FROM payment_pv;


-- Repeat Customer Rate
WITH df AS(
      SELECT a.masked_user_id,
             a.calendar_date,
             a.session_start_global_count,
             a.search_count,
             a.view_microsite_count,
             a.view_pdp_count,
             a.wishlist_count,
             a.input_kode_promo_count,
             a.pilih_metode_pembayaran_count,
             a.is_make_order,
             u.os,
             u.is_install,
             u.city,
             u.region
      FROM `bitlabs-dab.I_CID_02.activity` a
      LEFT JOIN `bitlabs-dab.I_CID_02.user` u
            ON a.masked_user_id = u.masked_user_id
      WHERE u.city IS NOT NULL AND u.region IS NOT NULL
), df2 AS(
      SELECT *
      FROM df
      WHERE city IS NOT NULL OR region IS NOT NULL
), df3 AS(
      SELECT *,
             EXTRACT (WEEK FROM calendar_date)-12 AS week
      FROM df2
), df4 AS(
      SELECT *,
             CASE 
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0) AND wishlist_count =0 AND input_kode_promo_count = 0 AND pilih_metode_pembayaran_count=0 AND is_make_order = False THEN 'View'
                  WHEN (session_start_global_count >=0 OR search_count >= 0 OR view_microsite_count >=0 OR view_pdp_count >= 0 OR wishlist_count>=0 OR input_kode_promo_count >= 0 OR pilih_metode_pembayaran_count>=0 ) AND is_make_order = False THEN 'Cart'
                  ELSE 'Purchase'
            END AS event_type
      FROM df3
), df5 AS(
      SELECT *,
             CASE 
                  WHEN is_install = False AND (os = 'Windows' OR os = 'Linux' OR os = 'Mac OS X' OR os='Chrome OS') THEN 'Web'
                  WHEN is_install = False AND (os = 'Android' OR os = 'iOS') THEN 'Mobile'
                  WHEN is_install = True THEN 'App'
                  ELSE 'Other'
            END AS type_device
      FROM df4
), num_trans AS(
   SELECT masked_user_id, COUNT(is_make_order) num_transs
   FROM df5
   WHERE is_make_order = True
   GROUP BY 1
), repeat_cust AS (
 SELECT COUNT(masked_user_id) num_repeat_cust
 FROM num_trans
 WHERE num_transs>1
)
 SELECT COUNT(a.masked_user_id) count_cust, b.num_repeat_cust count_repeat_cust, CONCAT(ROUND(b.num_repeat_cust/COUNT(a.masked_user_id)*100),'%') repeat_cust_rate
 FROM num_trans a, repeat_cust b
 GROUP BY 2
