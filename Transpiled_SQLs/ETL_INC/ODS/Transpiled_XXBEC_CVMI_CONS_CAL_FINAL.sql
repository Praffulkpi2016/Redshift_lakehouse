/* Delete Records */
DELETE FROM silver_bec_ods.XXBEC_CVMI_CONS_CAL_FINAL
WHERE
  (COALESCE(PART_NUMBER, 'NA'), COALESCE(ORGANIZATION_ID, 0), COALESCE(PLAN_NAME, 'NA'), COALESCE(BQOH, 0), COALESCE(CVMI_QOH, 0), COALESCE(AS_OF_DATE, '1900-01-01')) IN (
    SELECT
      COALESCE(stg.PART_NUMBER, 'NA') AS PART_NUMBER,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(stg.PLAN_NAME, 'NA') AS PLAN_NAME,
      COALESCE(stg.BQOH, 0) AS BQOH,
      COALESCE(stg.CVMI_QOH, 0) AS CVMI_QOH,
      COALESCE(stg.AS_OF_DATE, '1900-01-01') AS AS_OF_DATE
    FROM silver_bec_ods.XXBEC_CVMI_CONS_CAL_FINAL AS ods, bronze_bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL AS stg
    WHERE
      COALESCE(ods.PART_NUMBER, 'NA') = COALESCE(stg.PART_NUMBER, 'NA')
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND COALESCE(ods.PLAN_NAME, 'NA') = COALESCE(stg.PLAN_NAME, 'NA')
      AND COALESCE(ods.BQOH, 0) = COALESCE(stg.BQOH, 0)
      AND COALESCE(ods.CVMI_QOH, 0) = COALESCE(stg.CVMI_QOH, 0)
      AND COALESCE(ods.AS_OF_DATE, '1900-01-01') = COALESCE(stg.AS_OF_DATE, '1900-01-01')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.XXBEC_CVMI_CONS_CAL_FINAL (
  plan_name,
  ran_date,
  as_of_date,
  part_number,
  organization_id,
  demand,
  bqoh,
  cvmi_qoh,
  ncvmi_receipts,
  cvmi_receipts,
  remaining,
  demand_pulls,
  aging_pulls,
  future_consumptions,
  ending_bqoh,
  ending_cvmi_bqoh,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    plan_name,
    ran_date,
    as_of_date,
    part_number,
    organization_id,
    demand,
    bqoh,
    cvmi_qoh,
    ncvmi_receipts,
    cvmi_receipts,
    remaining,
    demand_pulls,
    aging_pulls,
    future_consumptions,
    ending_bqoh,
    ending_cvmi_bqoh,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(PART_NUMBER, 'NA'), COALESCE(ORGANIZATION_ID, 0), COALESCE(PLAN_NAME, 'NA'), COALESCE(BQOH, 0), COALESCE(CVMI_QOH, 0), COALESCE(AS_OF_DATE, '1900-01-01'), kca_seq_id) IN (
      SELECT
        COALESCE(PART_NUMBER, 'NA') AS PART_NUMBER,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(PLAN_NAME, 'NA') AS PLAN_NAME,
        COALESCE(BQOH, 0) AS BQOH,
        COALESCE(CVMI_QOH, 0) AS CVMI_QOH,
        COALESCE(AS_OF_DATE, '1900-01-01') AS AS_OF_DATE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(PART_NUMBER, 'NA'),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(PLAN_NAME, 'NA'),
        COALESCE(BQOH, 0),
        COALESCE(CVMI_QOH, 0),
        COALESCE(AS_OF_DATE, '1900-01-01')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.XXBEC_CVMI_CONS_CAL_FINAL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.XXBEC_CVMI_CONS_CAL_FINAL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(PART_NUMBER, 'NA'), COALESCE(ORGANIZATION_ID, 0), COALESCE(PLAN_NAME, 'NA'), COALESCE(BQOH, 0), COALESCE(CVMI_QOH, 0), COALESCE(AS_OF_DATE, '1900-01-01')) IN (
    SELECT
      COALESCE(PART_NUMBER, 'NA'),
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(PLAN_NAME, 'NA'),
      COALESCE(BQOH, 0),
      COALESCE(CVMI_QOH, 0),
      COALESCE(AS_OF_DATE, '1900-01-01')
    FROM bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
    WHERE
      (COALESCE(PART_NUMBER, 'NA'), COALESCE(ORGANIZATION_ID, 0), COALESCE(PLAN_NAME, 'NA'), COALESCE(BQOH, 0), COALESCE(CVMI_QOH, 0), COALESCE(AS_OF_DATE, '1900-01-01'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(PART_NUMBER, 'NA'),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(PLAN_NAME, 'NA'),
          COALESCE(BQOH, 0),
          COALESCE(CVMI_QOH, 0),
          COALESCE(AS_OF_DATE, '1900-01-01'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
        GROUP BY
          COALESCE(PART_NUMBER, 'NA'),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(PLAN_NAME, 'NA'),
          COALESCE(BQOH, 0),
          COALESCE(CVMI_QOH, 0),
          COALESCE(AS_OF_DATE, '1900-01-01')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_cvmi_cons_cal_final';