/* delete */
DELETE FROM gold_bec_dwh.DIM_PO_AP_METRIC
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.AP_INVOICES_ALL AS API, silver_bec_ods.AP_INVOICE_LINES_ALL AS APIL, silver_bec_ods.AP_INVOICE_PAYMENTS_ALL AS AIP
    WHERE
      1 = 1
      AND (
        APIL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_ap_metric' AND batch_name = 'po'
        )
        OR API.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_po_ap_metric' AND batch_name = 'po'
        )
      )
      AND API.CANCELLED_DATE IS NULL
      AND NOT APIL.PO_HEADER_ID IS NULL
      AND APIL.CANCELLED_FLAG = 'N'
      AND API.INVOICE_ID = APIL.INVOICE_ID
      AND API.ORG_ID = APIL.ORG_ID
      AND API.INVOICE_ID = AIP.INVOICE_ID()
      AND COALESCE(DIM_PO_AP_METRIC.ORG_ID, 0) = COALESCE(API.ORG_ID, 0)
      AND COALESCE(DIM_PO_AP_METRIC.INVOICE_ID, 0) = COALESCE(API.INVOICE_ID, 0)
      AND COALESCE(DIM_PO_AP_METRIC.PO_LINE_ID, 0) = COALESCE(APIL.PO_LINE_ID, 0)
      AND COALESCE(DIM_PO_AP_METRIC.PO_LINE_LOCATION_ID, 0) = COALESCE(APIL.PO_LINE_LOCATION_ID, 0)
  );
/* insert */
INSERT INTO gold_bec_dwh.DIM_PO_AP_METRIC
SELECT
  PO_HEADER_ID,
  PO_LINE_ID,
  PO_LINE_LOCATION_ID,
  INVOICE_ID,
  ORG_ID,
  SUM(INVOICE_AMOUNT) AS INVOICE_AMOUNT,
  SUM(AMOUNT_PAID) AS AMOUNT_PAID,
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
  ) || '-' || COALESCE(ORG_ID, 0) || '-' || COALESCE(INVOICE_ID, 0) || '-' || COALESCE(PO_LINE_ID, 0) || '-' || COALESCE(PO_LINE_LOCATION_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    APIL.PO_LINE_LOCATION_ID,
    APIL.PO_HEADER_ID,
    APIL.PO_LINE_ID,
    API.INVOICE_ID,
    API.ORG_ID,
    SUM(APIL.AMOUNT) AS INVOICE_AMOUNT,
    CASE
      WHEN CASE WHEN AIP.AMOUNT IS NULL THEN 'N' WHEN AIP.AMOUNT = 0 THEN 'N' ELSE 'Y' END = 'Y'
      THEN SUM(APIL.AMOUNT)
      ELSE 0
    END AS AMOUNT_PAID
  FROM silver_bec_ods.AP_INVOICES_ALL AS API, silver_bec_ods.AP_INVOICE_LINES_ALL AS APIL, silver_bec_ods.AP_INVOICE_PAYMENTS_ALL AS AIP
  WHERE
    1 = 1
    AND (
      APIL.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_ap_metric' AND batch_name = 'po'
      )
      OR API.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_ap_metric' AND batch_name = 'po'
      )
    )
    AND API.CANCELLED_DATE IS NULL
    AND NOT APIL.PO_HEADER_ID IS NULL
    AND APIL.CANCELLED_FLAG = 'N'
    AND API.INVOICE_ID = APIL.INVOICE_ID
    AND API.ORG_ID = APIL.ORG_ID
    AND API.INVOICE_ID = AIP.INVOICE_ID()
  GROUP BY
    APIL.PO_LINE_LOCATION_ID,
    APIL.PO_HEADER_ID,
    APIL.PO_LINE_ID,
    API.INVOICE_ID,
    API.ORG_ID,
    AIP.AMOUNT
)
GROUP BY
  PO_HEADER_ID,
  PO_LINE_ID,
  INVOICE_ID,
  PO_LINE_LOCATION_ID,
  ORG_ID;
/* soft DELETE */
UPDATE gold_bec_dwh.DIM_PO_AP_METRIC SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.AP_INVOICES_ALL AS API, silver_bec_ods.AP_INVOICE_LINES_ALL AS APIL, silver_bec_ods.AP_INVOICE_PAYMENTS_ALL AS AIP
    WHERE
      1 = 1
      AND (
        API.is_deleted_flg <> 'Y' OR APIL.is_deleted_flg <> 'Y'
      )
      AND API.CANCELLED_DATE IS NULL
      AND NOT APIL.PO_HEADER_ID IS NULL
      AND APIL.CANCELLED_FLAG = 'N'
      AND API.INVOICE_ID = APIL.INVOICE_ID
      AND API.ORG_ID = APIL.ORG_ID
      AND API.INVOICE_ID = AIP.INVOICE_ID()
      AND COALESCE(DIM_PO_AP_METRIC.ORG_ID, 0) = COALESCE(API.ORG_ID, 0)
      AND COALESCE(DIM_PO_AP_METRIC.INVOICE_ID, 0) = COALESCE(API.INVOICE_ID, 0)
      AND COALESCE(DIM_PO_AP_METRIC.PO_LINE_ID, 0) = COALESCE(APIL.PO_LINE_ID, 0)
      AND COALESCE(DIM_PO_AP_METRIC.PO_LINE_LOCATION_ID, 0) = COALESCE(APIL.PO_LINE_LOCATION_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_ap_metric' AND batch_name = 'po';