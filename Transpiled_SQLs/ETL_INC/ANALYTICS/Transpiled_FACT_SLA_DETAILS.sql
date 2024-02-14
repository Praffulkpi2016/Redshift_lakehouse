/* Delete Records */
DELETE FROM gold_bec_dwh.fact_sla_details
WHERE
  (COALESCE(ledger_id, 0), COALESCE(ae_header_id, 0), COALESCE(gl_sl_link_id, 0), COALESCE(je_header_id, 0)) IN (
    SELECT
      COALESCE(ods.ledger_id, 0) AS ledger_id,
      COALESCE(ods.ae_header_id, 0) AS ae_header_id,
      COALESCE(ods.gl_sl_link_id, 0) AS gl_sl_link_id,
      COALESCE(ods.je_header_id, 0) AS je_header_id
    FROM gold_bec_dwh.fact_sla_details AS dw, (
      SELECT
        xah.ledger_id,
        xal.ae_header_id,
        xal.ae_line_num,
        xal.gl_sl_link_id,
        XAH.je_category_name,
        xah.description AS header_description,
        xal.gl_transfer_mode_code,
        xah.accounting_entry_status_code,
        xah.gl_transfer_status_code,
        xal.party_type_code,
        xal.ACCOUNTING_CLASS_CODE,
        xal.description AS line_description,
        xal.accounted_cr,
        xal.accounted_dr,
        xah.period_name,
        xte.application_id,
        xte.source_id_int_1,
        xte.entity_id,
        xte.entity_code,
        gir.gl_sl_link_table,
        gir.je_header_id,
        gir.je_line_num
      FROM silver_bec_ods.gl_import_references AS gir, silver_bec_ods.xla_ae_lines AS xal, silver_bec_ods.xla_ae_headers AS xah, silver_bec_ods.xla_transaction_entities AS xte
      WHERE
        gir.gl_sl_link_id = xal.gl_sl_link_id
        AND gir.gl_sl_link_table = xal.gl_sl_link_table
        AND xal.ae_header_id = xah.ae_header_id
        AND xal.application_id = xah.application_id
        AND xal.ledger_id = xah.ledger_id
        AND xte.application_id = xah.application_id
        AND xte.entity_id = xah.entity_id
        AND xal.application_id = 200
        AND (
          gir.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
          )
          OR XAL.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
          )
          OR xah.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
          )
          OR xte.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
          )
          OR gir.IS_DELETED_FLG = 'Y'
          OR xal.IS_DELETED_FLG = 'Y'
          OR xah.IS_DELETED_FLG = 'Y'
          OR xte.IS_DELETED_FLG = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.ledger_id, 0) || '-' || COALESCE(ODS.ae_header_id, 0) || '-' || COALESCE(ODS.gl_sl_link_id, 0) || '-' || COALESCE(ODS.je_header_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_sla_details (
  ledger_id,
  ae_header_id,
  ae_line_num,
  gl_sl_link_id,
  je_category_name,
  header_description,
  gl_transfer_mode_code,
  accounting_entry_status_code,
  gl_transfer_status_code,
  party_type_code,
  accounting_class_code,
  line_description,
  accounted_cr,
  accounted_dr,
  period_name,
  application_id,
  source_id_int_1,
  entity_id,
  entity_code,
  gl_sl_link_table,
  je_header_id,
  je_line_num,
  ledger_id_key,
  ae_header_id_key,
  gl_sl_link_id_key,
  je_header_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    xah.ledger_id,
    xal.ae_header_id,
    xal.ae_line_num,
    xal.gl_sl_link_id,
    XAH.je_category_name,
    xah.description AS header_description,
    xal.gl_transfer_mode_code,
    xah.accounting_entry_status_code,
    xah.gl_transfer_status_code,
    xal.party_type_code,
    xal.ACCOUNTING_CLASS_CODE,
    xal.description AS line_description,
    xal.accounted_cr,
    xal.accounted_dr,
    xah.period_name,
    xte.application_id,
    xte.source_id_int_1,
    xte.entity_id,
    xte.entity_code,
    gir.gl_sl_link_table,
    gir.je_header_id,
    gir.je_line_num,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || xah.ledger_id AS ledger_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || xal.ae_header_id AS ae_header_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || xal.gl_sl_link_id AS gl_sl_link_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gir.je_header_id AS je_header_id_KEY,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(xah.ledger_id, 0) || '-' || COALESCE(xal.ae_header_id, 0) || '-' || COALESCE(xal.gl_sl_link_id, 0) || '-' || COALESCE(gir.je_header_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_import_references
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS gir, (
    SELECT
      *
    FROM silver_bec_ods.xla_ae_lines
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS xal, (
    SELECT
      *
    FROM silver_bec_ods.xla_ae_headers
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS xah, (
    SELECT
      *
    FROM silver_bec_ods.xla_transaction_entities
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS xte
  WHERE
    gir.gl_sl_link_id = xal.gl_sl_link_id
    AND gir.gl_sl_link_table = xal.gl_sl_link_table
    AND xal.ae_header_id = xah.ae_header_id
    AND xal.application_id = xah.application_id
    AND xal.ledger_id = xah.ledger_id
    AND xte.application_id = xah.application_id
    AND xte.entity_id = xah.entity_id
    AND xal.application_id = 200
    AND (
      gir.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
      )
      OR XAL.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
      )
      OR xah.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
      )
      OR xte.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_sla_details' AND batch_name = 'gl'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_sla_details' AND batch_name = 'gl';