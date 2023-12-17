CREATE SOURCE sale_events (
  id varchar,
  created_at TIMESTAMPTZ,
  customer_id int8,
  product_id int8,
  promotion_id int8,
  price decimal,
  quantity int8
 )
WITH (
 connector='kafka',
 topic='sale_events',
 properties.bootstrap.server='kafka_0:29092',
 scan.startup.mode='earliest'
 ) FORMAT PLAIN ENCODE JSON;
 

CREATE TABLE promotions (
  id int8,
  ad_type varchar,
  product_id int8,
  weight_percent int8,
  PRIMARY KEY (id)
);

CREATE MATERIALIZED VIEW promotions_sequences_60mins AS(
  SELECT 
    product_id, 
    promotion_id, 
    sum(total_price) AS sales_amount,
    window_start,
    window_end
  FROM (
    SELECT 
      product_id, 
      promotion_id, 
      price * quantity as total_price,
      window_start,
      window_end
    FROM TUMBLE (sale_events, created_at, INTERVAL '5 MINUTES')
  )
  WHERE window_start > NOW() - INTERVAL '60 minutes'
  GROUP BY window_start, window_end, product_id, promotion_id
  ORDER BY window_start DESC
);


CREATE MATERIALIZED VIEW promotions_stat_60mins AS 
(
  SELECT
    promotions.product_id AS product_id,
    promotion_id,
    SUM(sales_amount) / SUM(promotions.weight_percent) AS roii
  FROM promotions_sequences_60mins as seq
  JOIN promotions ON promotion_id = promotions.id AND seq.product_id = promotions.product_id
  GROUP BY product_id, promotion_id);

 
CREATE SINK promotions_stat_60mins_kafka FROM promotions_stat_60mins
WITH (
connector='kafka',
properties.bootstrap.server='kafka_0:29092',
topic='promotions_stat_60mins'
) FORMAT PLAIN ENCODE JSON (
force_append_only='true',
);