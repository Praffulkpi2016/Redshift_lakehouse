/* Delete Records */
DELETE FROM silver_bec_ods.WF_ACTIVITIES
WHERE
  (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0)) IN (
    SELECT
      COALESCE(stg.ITEM_TYPE, 'NA') AS ITEM_TYPE,
      COALESCE(stg.NAME, 'NA') AS NAME,
      COALESCE(stg.VERSION, 0) AS VERSION
    FROM silver_bec_ods.WF_ACTIVITIES AS ods, bronze_bec_ods_stg.WF_ACTIVITIES AS stg
    WHERE
      COALESCE(ods.ITEM_TYPE, 'NA') = COALESCE(stg.ITEM_TYPE, 'NA')
      AND COALESCE(ods.NAME, 'NA') = COALESCE(stg.NAME, 'NA')
      AND COALESCE(ods.VERSION, 0) = COALESCE(stg.VERSION, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WF_ACTIVITIES (
  item_type,
  `name`,
  version,
  `type`,
  rerun,
  expand_role,
  protect_level,
  custom_level,
  begin_date,
  end_date,
  `function`,
  result_type,
  cost,
  read_role,
  write_role,
  execute_role,
  icon_name,
  message,
  error_process,
  error_item_type,
  runnable_flag,
  function_type,
  event_name,
  direction,
  security_group_id,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    item_type,
    `name`,
    version,
    `type`,
    rerun,
    expand_role,
    protect_level,
    custom_level,
    begin_date,
    end_date,
    `function`,
    result_type,
    cost,
    read_role,
    write_role,
    execute_role,
    icon_name,
    message,
    error_process,
    error_item_type,
    runnable_flag,
    function_type,
    event_name,
    direction,
    security_group_id,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.WF_ACTIVITIES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ITEM_TYPE, 'NA') AS ITEM_TYPE,
        COALESCE(NAME, 'NA') AS NAME,
        COALESCE(VERSION, 0) AS VERSION,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.WF_ACTIVITIES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ITEM_TYPE, 'NA'),
        COALESCE(NAME, 'NA'),
        COALESCE(VERSION, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WF_ACTIVITIES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WF_ACTIVITIES SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0)) IN (
    SELECT
      COALESCE(ITEM_TYPE, 'NA'),
      COALESCE(NAME, 'NA'),
      COALESCE(VERSION, 0)
    FROM bec_raw_dl_ext.WF_ACTIVITIES
    WHERE
      (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ITEM_TYPE, 'NA'),
          COALESCE(NAME, 'NA'),
          COALESCE(VERSION, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WF_ACTIVITIES
        GROUP BY
          COALESCE(ITEM_TYPE, 'NA'),
          COALESCE(NAME, 'NA'),
          COALESCE(VERSION, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wf_activities';