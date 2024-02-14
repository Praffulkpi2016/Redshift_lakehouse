/*  DELETE RECORDS */
DELETE FROM gold_bec_dwh.DIM_EBS_APPLICATIONS
WHERE
  (COALESCE(APPLICATION_ID, 0), COALESCE(`LANGUAGE`, 'NA')) IN (
    SELECT
      COALESCE(ods.APPLICATION_ID, 0),
      COALESCE(ods.LANGUAGE, 'NA')
    FROM gold_bec_dwh.dim_ebs_applications AS dw, silver_bec_ods.FND_APPLICATION_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.APPLICATION_ID, 0) || '-' || COALESCE(ods.LANGUAGE, 'NA')
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ebs_applications' AND batch_name = 'gl'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_ebs_applications (
  APPLICATION_ID,
  `LANGUAGE`,
  APPLICATION_NAME,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  SOURCE_LANG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    APPLICATION_ID,
    `LANGUAGE`,
    APPLICATION_NAME,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    SOURCE_LANG,
    ZD_EDITION_NAME,
    ZD_SYNC,
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
    ) || '-' || COALESCE(APPLICATION_ID, 0) || '-' || COALESCE(`LANGUAGE`, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.FND_APPLICATION_TL
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ebs_applications' AND batch_name = 'gl'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ebs_applications SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(APPLICATION_ID, 0), COALESCE(`LANGUAGE`, 'NA')) IN (
    SELECT
      COALESCE(ods.APPLICATION_ID, 0),
      COALESCE(ods.LANGUAGE, 'NA')
    FROM gold_bec_dwh.dim_ebs_applications AS dw, silver_bec_ods.FND_APPLICATION_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.APPLICATION_ID, 0) || '-' || COALESCE(ods.LANGUAGE, 'NA')
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ebs_applications' AND batch_name = 'gl';