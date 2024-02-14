DROP table IF EXISTS gold_bec_dwh.DIM_AR_CONTACTS;
CREATE TABLE gold_bec_dwh.DIM_AR_CONTACTS AS
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
  FROM BEC_ODS.RA_CONTACTS AS con
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_contacts' AND batch_name = 'ar';