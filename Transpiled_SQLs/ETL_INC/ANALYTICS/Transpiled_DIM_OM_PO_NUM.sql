/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_OM_PO_NUM
WHERE
  (
    COALESCE(LINE_ID, 0)
  ) IN (
    SELECT
      ods.LINE_ID
    FROM gold_bec_dwh.DIM_OM_PO_NUM AS dw, (
      SELECT
        COALESCE(ODSS.LINE_ID, 0) AS LINE_ID
      FROM BEC_ODS.OE_DROP_SHIP_SOURCES AS ODSS, BEC_ODS.PO_HEADERS_ALL AS POH
      WHERE
        1 = 1
        AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID()
        AND (
          ODSS.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_om_po_num' AND batch_name = 'om'
          )
          OR POH.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_om_po_num' AND batch_name = 'om'
          )
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.LINE_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_OM_PO_NUM (
  line_id,
  po_header_id,
  po_number,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    ODSS.LINE_ID,
    ODSS.PO_HEADER_ID,
    POH.SEGMENT1 AS PO_NUMBER,
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
    ) || '-' || COALESCE(ODSS.LINE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.OE_DROP_SHIP_SOURCES AS ODSS, BEC_ODS.PO_HEADERS_ALL AS POH
  WHERE
    1 = 1
    AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID()
    AND (
      ODSS.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_om_po_num' AND batch_name = 'om'
      )
      OR POH.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_om_po_num' AND batch_name = 'om'
      )
    )
);
/* Soft DELETE */
UPDATE gold_bec_dwh.DIM_OM_PO_NUM SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(LINE_ID, 0)
  ) IN (
    SELECT
      ods.LINE_ID
    FROM gold_bec_dwh.DIM_OM_PO_NUM AS dw, (
      SELECT
        COALESCE(ODSS.LINE_ID, 0) AS LINE_ID
      FROM (
        SELECT
          *
        FROM BEC_ODS.OE_DROP_SHIP_SOURCES
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ODSS, (
        SELECT
          *
        FROM BEC_ODS.PO_HEADERS_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS POH
      WHERE
        1 = 1 AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID()
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.LINE_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_om_po_num' AND batch_name = 'om';