/* Delete Records */
DELETE FROM gold_bec_dwh.dim_wf_item_types
WHERE
  COALESCE(NAME, 'NA') IN (
    SELECT
      ods.NAME
    FROM gold_bec_dwh.dim_wf_item_types AS dw, (
      SELECT
        COALESCE(B.NAME, 'NA') AS NAME
      FROM BEC_ODS.WF_ITEM_TYPES AS B, BEC_ODS.WF_ITEM_TYPES_TL AS T
      WHERE
        B.NAME = T.NAME
        AND T.LANGUAGE = 'US'
        AND (
          CAST(SUBSTRING(CAST(B.kca_seq_id AS STRING), 1, 8) AS TIMESTAMP) > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_wf_item_types' AND batch_name = 'om'
          )
          OR CAST(SUBSTRING(CAST(T.kca_seq_id AS STRING), 1, 8) AS TIMESTAMP) > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_wf_item_types' AND batch_name = 'om'
          )
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.NAME, 'NA')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_wf_item_types (
  NAME,
  PROTECT_LEVEL,
  CUSTOM_LEVEL,
  WF_SELECTOR,
  READ_ROLE,
  WRITE_ROLE,
  EXECUTE_ROLE,
  PERSISTENCE_TYPE,
  PERSISTENCE_DAYS,
  DISPLAY_NAME,
  DESCRIPTION,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    B.NAME = T.NAME
    AND T.LANGUAGE = 'US'
    AND (
      CAST(SUBSTRING(CAST(B.kca_seq_id AS STRING), 1, 8) AS TIMESTAMP) > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_wf_item_types' AND batch_name = 'om'
      )
      OR CAST(SUBSTRING(CAST(T.kca_seq_id AS STRING), 1, 8) AS TIMESTAMP) > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_wf_item_types' AND batch_name = 'om'
      )
    )
);
/* Soft DELETE */
UPDATE gold_bec_dwh.dim_wf_item_types SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(NAME, 'NA') IN (
    SELECT
      ods.NAME
    FROM gold_bec_dwh.dim_wf_item_types AS dw, (
      SELECT
        COALESCE(B.NAME, 'NA') AS NAME
      FROM (
        SELECT
          *
        FROM BEC_ODS.WF_ITEM_TYPES
        WHERE
          is_deleted_flg <> 'Y'
      ) AS B, (
        SELECT
          *
        FROM BEC_ODS.WF_ITEM_TYPES_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS T
      WHERE
        B.NAME = T.NAME AND T.LANGUAGE = 'US'
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.NAME, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_wf_item_types' AND batch_name = 'om';