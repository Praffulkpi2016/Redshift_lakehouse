TRUNCATE table bronze_bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL;
INSERT INTO bronze_bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL (
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
  kca_seq_id,
  KCA_SEQ_DATE
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
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(PART_NUMBER, 'NA'), COALESCE(ORGANIZATION_ID, 0), COALESCE(PLAN_NAME, 'NA'), COALESCE(BQOH, 0), COALESCE(CVMI_QOH, 0), COALESCE(AS_OF_DATE, '1900-01-01'), kca_seq_id) IN (
      SELECT
        COALESCE(PART_NUMBER, 'NA') AS PART_NUMBER,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(PLAN_NAME, 'NA') AS PLAN_NAME,
        COALESCE(BQOH, 0) AS BQOH,
        COALESCE(CVMI_QOH, 0) AS CVMI_QOH,
        COALESCE(AS_OF_DATE, '1900-01-01') AS AS_OF_DATE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.XXBEC_CVMI_CONS_CAL_FINAL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(PART_NUMBER, 'NA'),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(PLAN_NAME, 'NA'),
        COALESCE(BQOH, 0),
        COALESCE(CVMI_QOH, 0),
        COALESCE(AS_OF_DATE, '1900-01-01')
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'xxbec_cvmi_cons_cal_final'
      )
    )
);