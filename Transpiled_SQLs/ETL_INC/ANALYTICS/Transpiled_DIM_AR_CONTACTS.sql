/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_CONTACTS
WHERE
  COALESCE(contact_id, 0) IN (
    SELECT
      COALESCE(ods.contact_id, 0)
    FROM gold_bec_dwh.DIM_AR_CONTACTS AS dw, silver_bec_ods.RA_CONTACTS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.contact_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_contacts' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_CONTACTS (
  contact_id,
  sold_to_contact_name,
  contact_number,
  email_address,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  job_title,
  creation_date,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    con.contact_id,
    con.first_name || ' ' || con.last_name AS `SOLD_TO_CONTACT_NAME`,
    con.contact_number,
    con.email_address,
    con.attribute1,
    con.attribute2,
    con.attribute3,
    con.attribute4,
    con.attribute5,
    con.job_title,
    TRUNC(con.creation_date) AS `CREATION_DATE`,
    TRUNC(con.last_update_date) AS `LAST_UPDATE_DATE`,
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
    ) || '-' || COALESCE(con.CONTACT_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.RA_CONTACTS AS con
  WHERE
    1 = 1
    AND (
      con.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_contacts' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_CONTACTS SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(contact_id, 0) IN (
    SELECT
      COALESCE(ods.contact_id, 0)
    FROM gold_bec_dwh.DIM_AR_CONTACTS AS dw, bec_raw_dl_ext.RA_CONTACTS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.contact_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_contacts' AND batch_name = 'ar';