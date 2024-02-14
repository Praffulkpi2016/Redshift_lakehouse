/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ap_notes
WHERE
  (COALESCE(note_id, 0), COALESCE(report_header_id, 0)) IN (
    SELECT
      COALESCE(ods.note_id, 0) AS note_id,
      COALESCE(ods.report_header_id, 0) AS report_header_id
    FROM gold_bec_dwh.dim_ap_notes AS dw, (
      SELECT
        an.note_id,
        aer.report_header_id
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
        AND (
          an.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ap_notes' AND batch_name = 'ap'
          )
          OR aer.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ap_notes' AND batch_name = 'ap'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.NOTE_ID, 0) || '-' || COALESCE(ods.REPORT_HEADER_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_ap_notes (
  note_id,
  report_header_id,
  org_id,
  invoice_num,
  entered_date,
  notes_detail,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    AND (
      an.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_notes' AND batch_name = 'ap'
      )
      OR aer.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_notes' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_notes SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(note_id, 0), COALESCE(report_header_id, 0)) IN (
    SELECT
      COALESCE(ods.note_id, 0) AS note_id,
      COALESCE(ods.report_header_id, 0) AS report_header_id
    FROM gold_bec_dwh.dim_ap_notes AS dw, (
      SELECT
        an.note_id,
        aer.report_header_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ap_notes
        WHERE
          is_deleted_flg <> 'Y'
      ) AS an, (
        SELECT
          *
        FROM silver_bec_ods.ap_expense_report_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS aer
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
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.NOTE_ID, 0) || '-' || COALESCE(ods.REPORT_HEADER_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_notes' AND batch_name = 'ap';