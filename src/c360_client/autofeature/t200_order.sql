WITH

order_table AS (
    SELECT
        *
    FROM
        ulph_c360_lake__tindahan_derived_restricted.t205_order_latest
    WHERE
        so_date >= DATE('2020-07-01')
),

area AS (
    SELECT * FROM ulph_c360_lake__distributor_source_restricted.t000_distributor_area_latest
),

customer_online_portion as (
  SELECT
    outlet,
    SUM(IF(order_type = 'ZRSS', gsv, 0)) / SUM(gsv) AS online_portion
  FROM
    ulph_c360_lake__distributor_derived_restricted.t200_dt_transaction_latest
  WHERE
    calendar_day > DATE('2021-01-01')
  GROUP BY
    outlet
),

order_table_warea AS (
    SELECT
        order_table.site_name,
        master_outlet_subtype_name,
        order_table.outlet,
        so_document,
        so_date,
        sku_name,
        pack_size,
        sales_order_qty_cs,
        ROUND(pack_size * sales_order_qty_cs) AS unit_count,
        -- invoiced_qty_in_cs,
        -- ordered_lines,
        -- invoiced_lines,
        ordered_amount,
        ordered_amount / ROUND(pack_size * sales_order_qty_cs) AS unit_price,
        -- invoiced_amount,
        brand,
        category,
        subcategory,
        -- gsv,
        region,
        area,
        distributor_name,
        online_portion
    FROM
        order_table
    LEFT JOIN
        area
    ON
        order_table.site = area.site AND order_table.site_name = order_table.site_name
    LEFT JOIN
        customer_online_portion
    ON
        order_table.outlet = customer_online_portion.outlet
)

SELECT * FROM order_table_warea
where online_portion >= 0.9
