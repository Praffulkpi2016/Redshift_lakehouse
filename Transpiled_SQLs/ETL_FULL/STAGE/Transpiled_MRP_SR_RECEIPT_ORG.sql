DROP table IF EXISTS bronze_bec_ods_stg.MRP_SR_RECEIPT_ORG;
CREATE TABLE bronze_bec_ods_stg.MRP_SR_RECEIPT_ORG AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
  WHERE
    kca_operation <> 'DELETE'
    AND (
      COALESCE(SR_RECEIPT_ID, 0)
    ) IN (
      SELECT
        COALESCE(SR_RECEIPT_ID, 0) AS SR_RECEIPT_ID
      FROM bec_raw_dl_ext.MRP_SR_RECEIPT_ORG
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(SR_RECEIPT_ID, 0)
    )
);