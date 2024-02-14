TRUNCATE table gold_bec_dwh_rpt.FACT_P2P_CYCLE_TIME_RT;
INSERT INTO gold_bec_dwh_rpt.FACT_P2P_CYCLE_TIME_RT
(
  SELECT
    org_id,
    check_id,
    invoice_id,
    po_header_id,
    vendor_id,
    vendor_site_id,
    check_number,
    check_date,
    check_amount,
    period_num,
    period_year,
    period_name,
    payment_due_date,
    invoice_type_lookup_code,
    invoice_num,
    invoice_amount,
    invoice_date,
    voucher_date,
    vendor_name,
    po_type,
    po_date,
    po_number,
    po_release_number,
    receipt_date,
    receipt_number,
    po_creation_date,
    DATEDIFF(INVOICE_DATE, PO_DATE) AS INV_PODT,
    DATEDIFF(RECEIPT_DATE, INVOICE_DATE) AS INV_TO_REPT,
    DATEDIFF(voucher_date, INVOICE_DATE) AS INV_TO_VOC,
    DATEDIFF(check_date, receipt_date) AS REC_TO_CHK,
    DATEDIFF(check_date, invoice_date) AS INV_TO_CHK,
    DATEDIFF(
      check_date,
      CASE
        WHEN invoice_type_lookup_code = 'EXPENSE REPORT'
        THEN (
          voucher_date + 15
        )
        ELSE (
          payment_due_date + 10
        )
      END
    ) AS CHK_TO_DUE,
    CASE
      WHEN SIGN(DATEDIFF(invoice_date, receipt_date) + 30) = -1
      OR (
        SIGN(DATEDIFF(invoice_date, receipt_date) + 30) IS NULL AND -1 IS NULL
      )
      THEN 1
      WHEN SIGN(DATEDIFF(invoice_date, receipt_date) + 30) = 0
      THEN 1
      ELSE 0
    END AS RCPT_30D,
    CASE
      WHEN SIGN(
        DATEDIFF(
          check_date,
          CASE
            WHEN invoice_type_lookup_code = 'EXPENSE REPORT'
            THEN (
              voucher_date + 15
            )
            ELSE (
              payment_due_date + 10
            )
          END
        )
      ) = -1
      OR (
        SIGN(
          DATEDIFF(
            check_date,
            CASE
              WHEN invoice_type_lookup_code = 'EXPENSE REPORT'
              THEN (
                voucher_date + 15
              )
              ELSE (
                payment_due_date + 10
              )
            END
          )
        ) IS NULL
        AND -1 IS NULL
      )
      THEN 1
      WHEN SIGN(
        DATEDIFF(
          check_date,
          CASE
            WHEN invoice_type_lookup_code = 'EXPENSE REPORT'
            THEN (
              voucher_date + 15
            )
            ELSE (
              payment_due_date + 10
            )
          END
        )
      ) = 0
      THEN 1
      ELSE 0
    END AS Payments_ontime,
    UNIX_TIMESTAMP(INVOICE_DATE) - UNIX_TIMESTAMP(PO_DATE) AS INV_PODT_SEC, /* Added for quicksight development */
    UNIX_TIMESTAMP(RECEIPT_DATE) - UNIX_TIMESTAMP(INVOICE_DATE) AS INV_TO_REPT_SEC,
    UNIX_TIMESTAMP(voucher_date) - UNIX_TIMESTAMP(INVOICE_DATE) AS INV_TO_VOC_SEC,
    UNIX_TIMESTAMP(check_date) - UNIX_TIMESTAMP(receipt_date) AS REC_TO_CHK_SEC,
    UNIX_TIMESTAMP(check_date) - UNIX_TIMESTAMP(invoice_date) AS INV_TO_CHK_SEC,
    UNIX_TIMESTAMP(check_date) - UNIX_TIMESTAMP(CASE
      WHEN invoice_type_lookup_code = 'EXPENSE REPORT'
      THEN (
        voucher_date + 15
      )
      ELSE (
        payment_due_date + 10
      )
    END) AS CHK_TO_DUE_SEC
  FROM (
    SELECT DISTINCT
      ac.org_id,
      ac.check_id,
      api.invoice_id,
      poh.po_header_id,
      api.vendor_id,
      api.vendor_site_id,
      ac.check_number,
      ac.check_date,
      ac.amount AS check_amount,
      gp.period_num,
      gp.period_year,
      gp.period_name,
      aps.due_date AS payment_due_date,
      api.invoice_type_lookup_code,
      api.invoice_num,
      api.invoice_amount,
      api.invoice_date,
      CASE
        WHEN api.invoice_type_lookup_code = 'EXPENSE REPORT'
        THEN exp_rep.entered_date
        ELSE api.creation_date
      END AS voucher_date,
      sup.vendor_name,
      poh.type_lookup_code AS po_type,
      CASE
        WHEN poh.type_lookup_code = 'STANDARD'
        THEN poh.APPROVED_DATE
        WHEN poh.type_lookup_code = 'BLANKET'
        THEN por.release_date
        ELSE poh.APPROVED_DATE
      END AS po_date,
      poh.segment1 AS po_number,
      por.release_num AS po_release_number,
      MAX(rsh.creation_date) AS receipt_date,
      MAX(rsh.receipt_num) AS receipt_number,
      poh.creation_date AS po_creation_date
    FROM (
      SELECT
        *
      FROM BEC_ODS.ap_checks_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ac, (
      SELECT
        *
      FROM BEC_ODS.gl_period_statuses
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gp, (
      SELECT
        *
      FROM BEC_ODS.ap_invoice_payments_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS aip, (
      SELECT
        *
      FROM BEC_ODS.ap_invoices_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS api, (
      SELECT
        *
      FROM BEC_ODS.ap_invoice_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS apil, (
      SELECT
        *
      FROM BEC_ODS.ap_payment_schedules_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS aps, (
      SELECT
        *
      FROM BEC_ODS.po_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poh, (
      SELECT
        *
      FROM BEC_ODS.po_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS pol, (
      SELECT
        *
      FROM BEC_ODS.po_releases_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS por, (
      SELECT
        *
      FROM BEC_ODS.po_line_locations_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poll, (
      SELECT
        *
      FROM BEC_ODS.rcv_shipment_lines
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rsl, (
      SELECT
        *
      FROM BEC_ODS.rcv_shipment_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rsh, (
      SELECT
        *
      FROM BEC_ODS.ap_suppliers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS sup, (
      SELECT
        aer.invoice_num,
        an.entered_date,
        an.notes_detail
      FROM (
        SELECT
          *
        FROM BEC_ODS.ap_notes
        WHERE
          is_deleted_flg <> 'Y'
      ) AS an, (
        SELECT
          *
        FROM BEC_ODS.ap_expense_report_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS aer
      WHERE
        1 = 1
        AND an.source_object_id = aer.report_header_id
        AND aer.org_id = 85
        AND an.notes_detail LIKE 'Approver Action: Approve%'
        AND an.entered_date = (
          SELECT
            MAX(entered_date)
          FROM (
            SELECT
              *
            FROM BEC_ODS.ap_notes
            WHERE
              is_deleted_flg <> 'Y'
          ) AS an1
          WHERE
            an1.source_object_id = an.source_object_id
            AND an1.notes_detail LIKE 'Approver Action: Approve%'
        )
    ) AS exp_rep
    WHERE
      FLOOR(ac.check_date) BETWEEN gp.start_date AND gp.end_date
      AND gp.application_id = 200
      AND gp.set_of_books_id = aip.set_of_books_id
      AND ac.check_id = aip.CHECK_ID()
      AND aip.invoice_id = apil.invoice_id
      AND apil.invoice_id = api.invoice_id
      AND api.invoice_id = aps.INVOICE_ID()
      AND apil.po_line_location_id = poll.LINE_LOCATION_ID()
      AND poll.po_line_id = pol.PO_LINE_ID()
      AND poll.po_release_id = por.PO_RELEASE_ID()
      AND pol.po_header_id = poh.PO_HEADER_ID()
      AND poll.line_location_id = rsl.PO_LINE_LOCATION_ID()
      AND rsl.shipment_header_id = rsh.SHIPMENT_HEADER_ID()
      AND api.invoice_num = exp_rep.INVOICE_NUM()
      AND api.vendor_id = sup.VENDOR_ID()
      AND ac.status_lookup_code <> 'VOIDED'
    GROUP BY
      ac.org_id,
      ac.check_id,
      api.invoice_id,
      poh.po_header_id,
      api.vendor_id,
      api.vendor_site_id,
      ac.check_number,
      ac.check_date,
      ac.amount,
      gp.period_num,
      gp.period_year,
      gp.period_name,
      aps.due_date,
      api.invoice_type_lookup_code,
      apil.line_type_lookup_code,
      api.invoice_num,
      api.invoice_amount,
      api.invoice_date,
      api.creation_date,
      exp_rep.entered_date,
      sup.vendor_name,
      poll.po_header_id,
      poll.po_release_id,
      poh.type_lookup_code,
      CASE
        WHEN poh.type_lookup_code = 'STANDARD'
        THEN poh.APPROVED_DATE
        WHEN poh.type_lookup_code = 'BLANKET'
        THEN por.release_date
        ELSE poh.APPROVED_DATE
      END,
      poh.segment1,
      por.release_num,
      poh.creation_date
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_p2p_cycle_time_rt' AND batch_name = 'ap';