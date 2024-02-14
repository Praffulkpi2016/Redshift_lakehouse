DROP table IF EXISTS bronze_bec_ods_stg.QP_QUALIFIERS;
CREATE TABLE bronze_bec_ods_stg.QP_QUALIFIERS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.QP_QUALIFIERS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(QUALIFIER_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(QUALIFIER_ID, 0) AS QUALIFIER_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.QP_QUALIFIERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(QUALIFIER_ID, 0)
    )
);