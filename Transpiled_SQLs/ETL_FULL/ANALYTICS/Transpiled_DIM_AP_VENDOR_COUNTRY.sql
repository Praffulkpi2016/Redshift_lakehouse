DROP table IF EXISTS gold_bec_dwh.DIM_AP_VENDOR_COUNTRY;
CREATE TABLE gold_bec_dwh.DIM_AP_VENDOR_COUNTRY AS
(
  SELECT
    vendor_id,
    VENDOR_NAME,
    territory_short_name,
    TERRITORY_CODE,
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
    ) || '-' || COALESCE(vendor_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      aps.vendor_id,
      aps.VENDOR_NAME,
      ft.territory_short_name,
      ft.TERRITORY_CODE
    FROM silver_bec_ods.hz_parties AS hp, silver_bec_ods.ap_suppliers AS aps, silver_bec_ods.fnd_territories_tl AS ft
    WHERE
      aps.party_id = hp.party_id
      AND NOT hp.country IS NULL
      AND hp.country = ft.territory_code
      AND ft.language = 'US'
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_vendor_country' AND batch_name = 'ascp';