TRUNCATE table gold_bec_dwh.DIM_AP_VENDOR_COUNTRY;
INSERT INTO gold_bec_dwh.DIM_AP_VENDOR_COUNTRY
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
    FROM (
      SELECT
        party_id,
        country
      FROM silver_bec_ods.hz_parties
      WHERE
        1 = 1 AND NOT country IS NULL AND is_deleted_flg <> 'Y'
    ) AS hp, (
      SELECT
        vendor_id,
        VENDOR_NAME,
        party_id
      FROM silver_bec_ods.ap_suppliers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS aps, (
      SELECT
        territory_code,
        territory_short_name
      FROM silver_bec_ods.fnd_territories_tl
      WHERE
        is_deleted_flg <> 'Y' AND language = 'US'
    ) AS ft
    WHERE
      aps.party_id = hp.party_id AND hp.country = ft.territory_code
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_vendor_country' AND batch_name = 'ascp';