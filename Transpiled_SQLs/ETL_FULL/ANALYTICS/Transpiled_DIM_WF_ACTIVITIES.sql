DROP table IF EXISTS gold_bec_dwh.DIM_WF_ACTIVITIES;
CREATE TABLE gold_bec_dwh.DIM_WF_ACTIVITIES AS
(
  SELECT
    B.ITEM_TYPE,
    B.NAME,
    B.VERSION,
    B.TYPE,
    B.RERUN,
    B.EXPAND_ROLE,
    B.PROTECT_LEVEL,
    B.CUSTOM_LEVEL,
    B.BEGIN_DATE,
    B.END_DATE,
    B.FUNCTION,
    B.RESULT_TYPE,
    B.COST,
    B.READ_ROLE,
    B.WRITE_ROLE,
    B.EXECUTE_ROLE,
    B.ICON_NAME,
    B.MESSAGE,
    B.ERROR_PROCESS,
    B.RUNNABLE_FLAG,
    B.ERROR_ITEM_TYPE,
    B.FUNCTION_TYPE,
    B.EVENT_NAME,
    B.DIRECTION,
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
    ) || '-' || COALESCE(B.ITEM_TYPE, 'NA') || '-' || COALESCE(B.NAME, 'NA') || '-' || COALESCE(B.VERSION, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.WF_ACTIVITIES AS B, BEC_ODS.WF_ACTIVITIES_TL AS T
  WHERE
    1 = 1
    AND T.LANGUAGE = 'US'
    AND B.TYPE = 'PROCESS'
    AND B.runnable_flag = 'Y'
    AND CURRENT_TIMESTAMP() BETWEEN B.begin_date AND COALESCE(B.end_date, CURRENT_TIMESTAMP())
    AND B.ITEM_TYPE = T.ITEM_TYPE
    AND B.NAME = T.NAME
    AND B.VERSION = T.VERSION
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_wf_activities' AND batch_name = 'om';