TRUNCATE table bronze_bec_ods_stg.CST_COST_ELEMENTS;
INSERT INTO bronze_bec_ods_stg.CST_COST_ELEMENTS (
  COST_ELEMENT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  COST_ELEMENT,
  DESCRIPTION,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    COST_ELEMENT_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    COST_ELEMENT,
    DESCRIPTION,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_COST_ELEMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(COST_ELEMENT_ID, '0'), kca_seq_id) IN (
      SELECT
        COALESCE(COST_ELEMENT_ID, '0') AS COST_ELEMENT_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.CST_COST_ELEMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(COST_ELEMENT_ID, '0')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_cost_elements'
    )
);