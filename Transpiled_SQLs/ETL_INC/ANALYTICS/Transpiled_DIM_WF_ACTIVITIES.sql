/* delete */
DELETE FROM gold_bec_dwh.DIM_WF_ACTIVITIES
WHERE
  (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0)) IN (
    SELECT
      ODS.ITEM_TYPE,
      ODS.NAME,
      ODS.VERSION
    FROM gold_bec_dwh.DIM_WF_ACTIVITIES AS dw, (
      SELECT
        COALESCE(B.ITEM_TYPE, 'NA') AS ITEM_TYPE,
        COALESCE(B.NAME, 'NA') AS NAME,
        COALESCE(B.VERSION, 0) AS VERSION
      FROM BEC_ODS.WF_ACTIVITIES AS B, BEC_ODS.WF_ACTIVITIES_TL AS T
      WHERE
        1 = 1
        AND B.ITEM_TYPE = T.ITEM_TYPE
        AND B.NAME = T.NAME
        AND B.VERSION = T.VERSION
        AND T.LANGUAGE = 'US'
        AND B.TYPE = 'PROCESS'
        AND B.runnable_flag = 'Y'
        AND CURRENT_TIMESTAMP() BETWEEN B.begin_date AND COALESCE(B.end_date, CURRENT_TIMESTAMP())
        AND B.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_wf_activities' AND batch_name = 'om'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ITEM_TYPE, 'NA') || '-' || COALESCE(ods.NAME, 'NA') || '-' || COALESCE(ods.VERSION, 0)
  );
/* INSERT */
INSERT INTO gold_bec_dwh.DIM_WF_ACTIVITIES
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
  FROM (
    SELECT
      *
    FROM BEC_ODS.WF_ACTIVITIES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS B, (
    SELECT
      *
    FROM BEC_ODS.WF_ACTIVITIES_TL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS T
  WHERE
    B.ITEM_TYPE = T.ITEM_TYPE
    AND B.NAME = T.NAME
    AND B.VERSION = T.VERSION
    AND T.LANGUAGE = 'US'
    AND B.TYPE = 'PROCESS'
    AND B.runnable_flag = 'Y'
    AND CURRENT_TIMESTAMP() BETWEEN B.begin_date AND COALESCE(B.end_date, CURRENT_TIMESTAMP())
    AND B.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_wf_activities' AND batch_name = 'om'
    )
);
/* soft delete */
UPDATE gold_bec_dwh.DIM_WF_ACTIVITIES SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0)) IN (
    SELECT
      ODS.ITEM_TYPE,
      ODS.NAME,
      ODS.VERSION
    FROM gold_bec_dwh.DIM_WF_ACTIVITIES AS dw, (
      SELECT
        COALESCE(B.ITEM_TYPE, 'NA') AS ITEM_TYPE,
        COALESCE(B.NAME, 'NA') AS NAME,
        COALESCE(B.VERSION, 0) AS VERSION
      FROM (
        SELECT
          *
        FROM BEC_ODS.WF_ACTIVITIES
        WHERE
          is_deleted_flg <> 'Y'
      ) AS B, (
        SELECT
          *
        FROM BEC_ODS.WF_ACTIVITIES_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS T
      WHERE
        1 = 1
        AND B.ITEM_TYPE = T.ITEM_TYPE
        AND B.NAME = T.NAME
        AND B.VERSION = T.VERSION
        AND T.LANGUAGE = 'US'
        AND B.TYPE = 'PROCESS'
        AND B.runnable_flag = 'Y'
        AND CURRENT_TIMESTAMP() BETWEEN B.begin_date AND COALESCE(B.end_date, CURRENT_TIMESTAMP())
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ITEM_TYPE, 'NA') || '-' || COALESCE(ods.NAME, 'NA') || '-' || COALESCE(ods.VERSION, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_wf_activities' AND batch_name = 'om';