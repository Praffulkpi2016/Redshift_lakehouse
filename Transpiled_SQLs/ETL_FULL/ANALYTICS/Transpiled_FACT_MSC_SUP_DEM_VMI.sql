DROP table IF EXISTS gold_bec_dwh.FACT_MSC_SUP_DEM_VMI;
CREATE TABLE gold_bec_dwh.FACT_MSC_SUP_DEM_VMI AS
(
  SELECT
    plan_id,
    sr_instance_id,
    customer_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || customer_id AS CUSTOMER_ID_KEY,
    customer_site_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || customer_site_id AS CUSTOMER_SITE_ID_KEY,
    customer_name,
    customer_site_name,
    supplier_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || supplier_id AS SUPPLIER_ID_KEY,
    supplier_site_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || supplier_site_id AS SUPPLIER_SITE_ID_KEY,
    supplier_name,
    supplier_site_name,
    inventory_item_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS INVENTORY_ITEM_ID_KEY,
    item_name,
    description,
    order_details,
    planner_code,
    organization_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS ORGANIZATION_ID_KEY,
    vmi_type,
    aps_supplier_id,
    aps_supplier_site_id,
    aps_customer_id,
    aps_customer_site_id,
    buyer_code,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 1))) AS DECIMAL(28, 2)) AS Replenishment_Qty,
    TO_DATE(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 2))), 'DD-MON-YYYY') AS Replenishment_Date,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 3))) AS DECIMAL(28, 2)) AS Ordered_Qty,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 4))) AS DECIMAL(28, 2)) AS Onhand_Qty,
    TO_DATE(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 5))), 'DD-MON-YYYY') AS Onhand_Date,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 6))) AS DECIMAL(15, 0)) AS segment6,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 7))) AS DECIMAL(28, 2)) AS Intransit_Qty,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 8))) AS STRING) AS `PO_Number.ASN_Num`,
    TO_DATE(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 9))), 'DD-MON-YYYY') AS Expected_Receipt_Date,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 29))) AS DECIMAL(28, 2)) AS Max_Value,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 28))) AS DECIMAL(28, 2)) AS Min_Value,
    CAST(NULLIF(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 27))), '') AS DECIMAL(28, 2)) AS Receipt_Qty,
    CAST(LTRIM(RTRIM(SPLIT_PART(order_details, '#', 24))) AS DECIMAL(28, 2)) AS Process_Qty,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 10))) AS segment10,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 11))) AS segment11,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 12))) AS segment12,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 13))) AS segment13,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 14))) AS segment14,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 15))) AS segment15,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 16))) AS segment16,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 17))) AS segment17,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 18))) AS segment18,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 19))) AS segment19,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 20))) AS segment20,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 21))) AS segment21,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 22))) AS segment22,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 23))) AS segment23,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 25))) AS segment25,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 26))) AS segment26,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 30))) AS segment30,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 31))) AS segment31,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 32))) AS segment32,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 33))) AS segment33,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 34))) AS segment34,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 35))) AS segment35,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 36))) AS segment36,
    LTRIM(RTRIM(SPLIT_PART(order_details, '#', 37))) AS segment37,
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(INVENTORY_ITEM_ID, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(CUSTOMER_ID, 0) || '-' || COALESCE(CUSTOMER_SITE_ID, 0) || '-' || COALESCE(SUPPLIER_SITE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MSC_SUP_DEM_ENTRIES_VMI_V
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_msc_sup_dem_vmi' AND batch_name = 'inv';