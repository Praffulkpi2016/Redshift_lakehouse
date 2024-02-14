TRUNCATE table gold_bec_dwh.DIM_ASCP_ORGANIZATIONS;
INSERT INTO gold_bec_dwh.DIM_ASCP_ORGANIZATIONS
(
  SELECT
    partner_id,
    sr_tp_id AS organization_id,
    sr_instance_id,
    organization_code,
    master_organization,
    partner_name,
    OPERATING_UNIT,
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
    ) || '-' || COALESCE(partner_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.msc_trading_partners
  WHERE
    is_deleted_flg <> 'Y' AND partner_type = 3
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ascp_organizations' AND batch_name = 'ascp';