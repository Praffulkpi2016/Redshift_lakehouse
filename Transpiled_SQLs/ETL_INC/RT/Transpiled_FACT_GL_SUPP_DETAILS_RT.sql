TRUNCATE table gold_bec_dwh_rpt.FACT_GL_SUPP_DETAILS_RT;
WITH AP_PO AS (
  SELECT DISTINCT
    api.invoice_id,
    poh.segment1
  FROM silver_bec_ods.ap_invoice_lines_all AS api, silver_bec_ods.po_headers_all AS poh
  WHERE
    api.po_header_id = poh.po_header_id AND api.org_id = poh.org_id
)
INSERT INTO gold_bec_dwh_rpt.FACT_GL_SUPP_DETAILS_RT
(
  SELECT
    gl.ledger_id,
    gl.je_header_id,
    gl.je_line_num,
    gl.code_combination_id,
    apgl.ae_header_id,
    apgl.ae_line_num,
    apgl.gl_sl_link_id,
    gl.account,
    gl.batch_description,
    gl.batch_name,
    gl.budget_id,
    gl.company,
    gl.line_creation_date AS creation_date,
    gl.department,
    gl.external_reference,
    gl.header_description,
    gl.je_category,
    gl.journal_name AS je_header_name,
    gl.journal_source AS je_source,
    gl.line_description,
    gl.period_name,
    gl.posted_by,
    gl.posted_date,
    gl.project_number,
    gl.running_total_accounted_cr,
    gl.running_total_accounted_dr,
    gl.journal_header_status AS status,
    gl.task_number,
    COALESCE(
      CASE WHEN apgl.accounted_cr IS NULL THEN gl.accounted_cr ELSE apgl.accounted_cr END,
      0
    ) AS accounted_cr,
    COALESCE(
      CASE WHEN apgl.accounted_dr IS NULL THEN gl.accounted_dr ELSE apgl.accounted_dr END,
      0
    ) AS accounted_dr,
    api.invoice_num,
    (
      SELECT
        GROUP_CONCAT(segment1, ', ') WITHIN GROUP (ORDER BY
          invoice_id NULLS LAST)
      FROM AP_PO
      WHERE
        INVOICE_ID = API.INVOICE_ID
      GROUP BY
        INVOICE_ID
    ) AS po_number, /* xxbec_get_po_number(inv.invoice_id)                               */
    api.vendor_id,
    das.vendor_name,
    das.vendor_number
  FROM gold_bec_dwh.fact_gl_journals AS gl, gold_bec_dwh.fact_sla_details AS apgl, silver_bec_ods.ap_invoices_all AS api, gold_bec_dwh.dim_ap_suppliers AS das
  WHERE
    gl.je_header_id = apgl.JE_HEADER_ID()
    AND gl.je_line_num = apgl.JE_LINE_NUM()
    AND gl.ledger_id = apgl.LEDGER_ID()
    AND gl.period_name = apgl.PERIOD_NAME()
    AND apgl.source_id_int_1 = api.INVOICE_ID()
    AND api.vendor_id = das.VENDOR_ID()
    AND apgl.APPLICATION_ID() = 200
    AND apgl.GL_TRANSFER_MODE_CODE() = 'S'
    AND apgl.ACCOUNTING_ENTRY_STATUS_CODE() = 'F'
    AND apgl.GL_TRANSFER_STATUS_CODE() = 'Y'
    AND apgl.PARTY_TYPE_CODE() = 'S'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_gl_supp_details_rt' AND batch_name = 'gl';