DROP table IF EXISTS bronze_bec_ods_stg.MTL_ABC_CLASSES;
CREATE TABLE bronze_bec_ods_stg.MTL_ABC_CLASSES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_ABC_CLASSES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ABC_CLASS_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ABC_CLASS_ID, 0) AS ABC_CLASS_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_ABC_CLASSES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ABC_CLASS_ID, 0)
    )
);