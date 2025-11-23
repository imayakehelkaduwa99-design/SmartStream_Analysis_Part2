CREATE OR REPLACE TABLE `retail_project2_extended.customer_cleaned` AS
SELECT
  SAFE_CAST(Customer_ID AS INT64) AS customer_id,
  SAFE_CAST(Chatbot_Usage_Count AS INT64) AS chatbot_usage_count,
  SAFE_CAST(Last_Chatbot_Interaction AS DATE) AS last_chatbot_interaction,
  SAFE_CAST(Email_Opened_Count AS INT64) AS email_opened_count,
  SAFE_CAST(Clicked_Ad_Campaigns AS INT64) AS clicked_ad_campaigns,
  SAFE_CAST(Participated_in_Survey AS BOOL) AS participated_in_survey,
  Preferred_Channel AS preferred_channel,
  Loyalty_Program_Status AS loyalty_program_status,
  Marketing_Responsiveness AS marketing_responsiveness,
  Referral_Likelihood AS referral_likelihood,
  Gender AS gender,
  SAFE_CAST(Tenure_Months AS INT64) AS tenure_months
FROM `retail_project2_extended.customer_raw`;

CREATE OR REPLACE TABLE `retail_project2_extended.transaction_cleaned` AS
SELECT
  SAFE_CAST(Transaction_ID AS STRING) AS transaction_id,
  SAFE_CAST(Customer_ID AS INT64) AS customer_id,
  SAFE_CAST(Transaction_Date AS DATE) AS transaction_date,
  Product_SKU AS product_sku,
  Product_Description AS product_description,
  Product_Category AS product_category,
  SAFE_CAST(Quantity AS INT64) AS quantity,
  SAFE_CAST(Avg_Price AS FLOAT64) AS avg_price,
  SAFE_CAST(Delivery_Charges AS FLOAT64) AS delivery_charges,
  Coupon_Status AS coupon_status,
  Coupon_Code AS coupon_code,
  SAFE_CAST(Discount_pct AS FLOAT64) AS discount_pct,
  Payment_Method AS payment_method,
  Shipping_Provider AS shipping_provider,
  SAFE_CAST(Transaction_Rating AS FLOAT64) AS transaction_rating,

  -- Calculate net revenue
  SAFE_CAST(Quantity AS INT64)
    * SAFE_CAST(Avg_Price AS FLOAT64)
    * (1 - SAFE_CAST(Discount_pct AS FLOAT64) / 100.0) AS net_revenue

FROM `retail_project2_extended.transaction_raw`;

CREATE OR REPLACE TABLE `retail_project2_extended.customer_transactions` AS
SELECT
  t.*,
  c.chatbot_usage_count,
  c.last_chatbot_interaction,
  c.email_opened_count,
  c.clicked_ad_campaigns,
  c.participated_in_survey,
  c.preferred_channel,
  c.loyalty_program_status,
  c.marketing_responsiveness,
  c.referral_likelihood,
  c.gender,
  c.tenure_months
FROM `retail_project2_extended.transaction_cleaned` t
LEFT JOIN `retail_project2_extended.customer_cleaned` c
ON t.customer_id = c.customer_id;


SELECT COUNT(*) AS customers
FROM `retail_project2_extended.customer_cleaned`;

SELECT COUNT(*) AS transactions
FROM `retail_project2_extended.transaction_cleaned`;


SELECT COUNT(*) AS joined_rows
FROM `retail_project2_extended.customer_transactions`;


SELECT
  loyalty_program_status,

  -- Average annual revenue per customer
  AVG(net_revenue) AS avg_revenue_per_transaction,
  SUM(net_revenue) / COUNT(DISTINCT customer_id) AS avg_revenue_per_customer,

  -- Average order frequency
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_order_frequency,

  -- Average discount usage
  AVG(discount_pct) AS avg_discount_used,

  -- Number of transactions per customer
  COUNT(transaction_id) AS total_transactions,
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_transactions_per_customer,

  -- Customer engagement metrics
  AVG(email_opened_count) AS avg_email_opens,
  AVG(clicked_ad_campaigns) AS avg_ad_clicks,
  AVG(chatbot_usage_count) AS avg_chatbot_usage,

  -- Experience metrics
  AVG(transaction_rating) AS avg_rating,
  AVG(delivery_charges) AS avg_delivery_charges,

  -- Most common preferred channel
  ARRAY_AGG(preferred_channel ORDER BY preferred_channel LIMIT 1)[OFFSET(0)]
      AS most_common_preferred_channel,

  -- % survey participation
  AVG(CASE WHEN participated_in_survey = TRUE THEN 1 ELSE 0 END) * 100
      AS pct_survey_participation,

  -- % likely/very likely to refer
  AVG(CASE WHEN referral_likelihood IN ("Likely", "Very Likely")
           THEN 1 ELSE 0 END) * 100 AS pct_likely_to_refer

FROM `retail_project2_extended.customer_transactions`
GROUP BY loyalty_program_status
ORDER BY loyalty_program_status;

SELECT
  preferred_channel,
  SUM(net_revenue) AS total_revenue,
  AVG(net_revenue) AS avg_revenue_per_transaction,
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_order_frequency,
  AVG(transaction_rating) AS avg_transaction_rating,
  AVG(discount_pct) AS avg_discount_pct,
  AVG(CASE WHEN coupon_status = "Used" THEN 1 ELSE 0 END) * 100 AS coupon_usage_pct
FROM `retail_project2_extended.customer_transactions`
GROUP BY preferred_channel
ORDER BY avg_revenue_per_transaction DESC;

SELECT
  marketing_responsiveness,
  AVG(net_revenue) AS avg_revenue,
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_frequency,
  AVG(discount_pct) AS avg_discount,
  AVG(CASE WHEN coupon_status = "Used" THEN 1 ELSE 0 END) * 100 AS pct_coupon_usage,
  AVG(transaction_rating) AS avg_rating,
  
  -- channel distribution
  COUNTIF(preferred_channel = "Email") / COUNT(*) AS pct_email,
  COUNTIF(preferred_channel = "SMS") / COUNT(*) AS pct_sms,
  COUNTIF(preferred_channel = "Chatbot") / COUNT(*) AS pct_chatbot,
  COUNTIF(preferred_channel = "Social Media") / COUNT(*) AS pct_social

FROM `retail_project2_extended.customer_transactions`
GROUP BY marketing_responsiveness
ORDER BY avg_revenue DESC;

SELECT
  referral_likelihood,
  AVG(net_revenue) AS avg_revenue,
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_frequency,
  AVG(email_opened_count) AS avg_email_engagement,
  AVG(clicked_ad_campaigns) AS avg_ad_clicks,
  AVG(chatbot_usage_count) AS avg_chatbot_usage,
  
  -- product categories bought
  ARRAY_AGG(DISTINCT product_category) AS categories_bought

FROM `retail_project2_extended.customer_transactions`
GROUP BY referral_likelihood
ORDER BY avg_revenue DESC;


SELECT
  gender,
  SUM(net_revenue) AS total_revenue,
  AVG(net_revenue) AS avg_revenue,
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_orders,
  AVG(email_opened_count) AS avg_email_opens,
  AVG(clicked_ad_campaigns) AS avg_ad_clicks,
  AVG(chatbot_usage_count) AS avg_chatbot_usage,
  ARRAY_AGG(DISTINCT preferred_channel) AS channels_used,
  ARRAY_AGG(DISTINCT product_category) AS category_preferences
FROM `retail_project2_extended.customer_transactions`
GROUP BY gender;

CREATE OR REPLACE TABLE `retail_project2_extended.tenure_groups` AS
SELECT *,
  CASE
    WHEN tenure_months BETWEEN 0 AND 12 THEN "0–12"
    WHEN tenure_months BETWEEN 13 AND 24 THEN "13–24"
    WHEN tenure_months BETWEEN 25 AND 36 THEN "25–36"
    ELSE "37+"
  END AS tenure_group
FROM `retail_project2_extended.customer_transactions`;

SELECT
  tenure_group,
  AVG(net_revenue) AS avg_revenue,
  COUNT(transaction_id) / COUNT(DISTINCT customer_id) AS avg_orders,
  AVG(discount_pct) AS avg_discount,
  AVG(email_opened_count) AS avg_email,
  AVG(clicked_ad_campaigns) AS avg_clicks,
  AVG(chatbot_usage_count) AS avg_chatbot_usage,
  AVG(transaction_rating) AS avg_rating
FROM `retail_project2_extended.tenure_groups`
GROUP BY tenure_group
ORDER BY tenure_group;

SELECT
  loyalty_program_status,
  AVG(CASE WHEN coupon_status = "Used" THEN 1 ELSE 0 END) * 100 AS pct_coupon_usage,
  AVG(discount_pct) AS avg_discount_given,
  AVG(net_revenue) AS avg_revenue,
  COUNT(transaction_id) AS total_orders
FROM `retail_project2_extended.customer_transactions`
GROUP BY loyalty_program_status
ORDER BY avg_discount_given DESC;

SELECT
  loyalty_program_status,
  product_category,
  SUM(net_revenue) AS category_revenue,
  COUNT(*) AS total_transactions
FROM `retail_project2_extended.customer_transactions`
GROUP BY loyalty_program_status, product_category
ORDER BY loyalty_program_status, category_revenue DESC;

SELECT
  gender,
  product_category,
  SUM(net_revenue) AS total_revenue
FROM `retail_project2_extended.customer_transactions`
GROUP BY gender, product_category;

SELECT
  preferred_channel,
  product_category,
  SUM(net_revenue) AS revenue
FROM `retail_project2_extended.customer_transactions`
GROUP BY preferred_channel, product_category;


SELECT loyalty_program_status, AVG(transaction_rating) AS avg_rating
FROM `retail_project2_extended.customer_transactions`
GROUP BY loyalty_program_status;


SELECT preferred_channel, AVG(transaction_rating) AS avg_rating
FROM `retail_project2_extended.customer_transactions`
GROUP BY preferred_channel;


SELECT product_category, AVG(transaction_rating) AS avg_rating
FROM `retail_project2_extended.customer_transactions`
GROUP BY product_category;


SELECT shipping_provider, AVG(transaction_rating) AS avg_rating
FROM `retail_project2_extended.customer_transactions`
GROUP BY shipping_provider;


SELECT payment_method, AVG(transaction_rating) AS avg_rating
FROM `retail_project2_extended.customer_transactions`
GROUP BY payment_method;


WITH customer_spend AS (
  SELECT
    customer_id,
    SUM(net_revenue) AS total_revenue
  FROM `retail_project2_extended.customer_transactions`
  GROUP BY customer_id
),
threshold AS (
  SELECT
    APPROX_TOP_COUNT(total_revenue, 1)[OFFSET(0)].value AS cutoff
  FROM customer_spend
)

SELECT
  ct.*
FROM `retail_project2_extended.customer_transactions` ct
JOIN customer_spend s ON ct.customer_id = s.customer_id
WHERE s.total_revenue >= (SELECT cutoff FROM threshold);

WITH last_purchase AS (
  SELECT
    customer_id,
    MAX(transaction_date) AS last_date
  FROM `retail_project2_extended.customer_transactions`
  GROUP BY customer_id
),
ranked AS (
  SELECT
    customer_id,
    last_date,
    NTILE(5) OVER (ORDER BY last_date ASC) AS recency_bucket
  FROM last_purchase
)

SELECT *
FROM ranked
WHERE recency_bucket = 1;

SELECT
  customer_id,
  email_opened_count,
  chatbot_usage_count,
  preferred_channel
FROM `retail_project2_extended.customer_cleaned`
WHERE email_opened_count < 2 AND chatbot_usage_count > 5;

SELECT
  customer_id,
  COUNTIF(coupon_status = "Used") AS coupons_used,
  SUM(net_revenue) AS revenue,
  AVG(transaction_rating) AS avg_rating,
  ARRAY_AGG(DISTINCT product_category) AS categories
FROM `retail_project2_extended.customer_transactions`
GROUP BY customer_id;

SELECT
  shipping_provider,
  AVG(transaction_rating) AS avg_rating,
  COUNT(*) AS num_transactions
FROM `retail_project2_extended.customer_transactions`
GROUP BY shipping_provider;

SELECT
  CORR(discount_pct, net_revenue) AS discount_revenue_correlation
FROM `retail_project2_extended.customer_transactions`;
