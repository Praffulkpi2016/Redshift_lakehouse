DROP table IF EXISTS gold_bec_dwh.DIM_WF_ITEM_TYPES;
CREATE TABLE gold_bec_dwh.DIM_WF_ITEM_TYPES AS
(
  SELECT
    B.NAME,
    B.PROTECT_LEVEL,
    B.CUSTOM_LEVEL,
    B.WF_SELECTOR,
    B.READ_ROLE,
    B.WRITE_ROLE,
    B.EXECUTE_ROLE,
    B.PERSISTENCE_TYPE,
    B.PERSISTENCE_DAYS,
    T.DISPLAY_NAME,
    T.DESCRIPTION,
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
    ) || '-' || COALESCE(B.NAME, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.WF_ITEM_TYPES AS B, BEC_ODS.WF_ITEM_TYPES_TL AS T
  WHERE
    B.NAME = T.NAME AND T.LANGUAGE = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_wf_item_types' AND batch_name = 'om';