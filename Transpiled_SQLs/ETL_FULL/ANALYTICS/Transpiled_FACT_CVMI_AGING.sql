DROP table IF EXISTS gold_bec_dwh.FACT_CVMI_AGING;
CREATE TABLE gold_bec_dwh.FACT_CVMI_AGING AS
(
  SELECT
    a.as_of_date,
    a.ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || OOD.OPERATING_UNIT AS ORG_ID_KEY,
    OOD.OPERATING_UNIT AS ORG_ID,
    a.VENDOR_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.VENDOR_ID AS VENDOR_ID_KEY,
    a.VENDOR_SITE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    a.ITEM_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.ITEM_ID AS ITEM_ID_KEY,
    a.PRIMARY_VENDOR_ITEM,
    a.NAME,
    a.VENDORNAME,
    a.VENDORSITECODE,
    a.ITEMS,
    a.DESCRIPTION,
    a.UOM,
    a.SUBINVENTORY,
    a.LOCATOR,
    a.REVISION,
    a.RECEIPT_DATE,
    a.CONSUMEBEFORE,
    a.QUANTITY,
    a.AGING_PERIOD,
    a.aging_days,
    a.aging_bucket,
    a.RECEIVED_DATE_TIME,
    a.serial_lot_number,
    a.unit_price,
    a.extended_cost,
    a.percent_aging,
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
    ) || '-' || COALESCE(a.item_id, 0) || '-' || COALESCE(a.serial_lot_number, 'NA') || '-' || COALESCE(a.received_date_time, 'NA') || '-' || COALESCE(a.SUBINVENTORY, 'NA') || '-' || COALESCE(a.LOCATOR, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.BEC_CVMI_AGING_VIEW
    WHERE
      is_deleted_flg <> 'Y'
  ) AS a, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood
  WHERE
    a.organization_id = ood.organization_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cvmi_aging' AND batch_name = 'po';