DROP table IF EXISTS gold_bec_dwh.DIM_AP_NOTES;
CREATE TABLE gold_bec_dwh.DIM_AP_NOTES AS
(
  SELECT
    an.note_id,
    aer.REPORT_HEADER_ID,
    aer.org_id,
    aer.invoice_num,
    an.entered_date,
    an.notes_detail,
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
    ) || '-' || COALESCE(an.NOTE_ID, 0) || '-' || COALESCE(aer.REPORT_HEADER_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ap_notes AS an, silver_bec_ods.ap_expense_report_headers_all AS aer
  WHERE
    1 = 1
    AND an.source_object_id = aer.report_header_id
    AND an.notes_detail LIKE 'Approver Action: Approve%'
    AND an.entered_date = (
      SELECT
        MAX(entered_date)
      FROM silver_bec_ods.ap_notes AS an1
      WHERE
        an1.source_object_id = an.source_object_id
        AND an1.notes_detail LIKE 'Approver Action: Approve%'
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_notes' AND batch_name = 'ap';