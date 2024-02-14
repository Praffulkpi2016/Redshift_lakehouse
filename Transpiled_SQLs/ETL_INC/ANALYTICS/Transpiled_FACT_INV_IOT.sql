/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_INV_IOT
WHERE
  COALESCE(shipment_line_id, 0) IN (
    SELECT
      COALESCE(ods.shipment_line_id, 0) AS shipment_line_id
    FROM gold_bec_dwh.FACT_INV_IOT AS dw, (
      SELECT
        rch.shipment_header_id,
        rcl.shipment_line_id,
        rcl.item_id,
        rch.organization_id,
        rcl.mmt_transaction_id,
        rch.shipment_num AS iot, /*	 ,a.rid rid */
        msib.segment1 AS part_number,
        REGEXP_REPLACE(msib.description, '[^0-9A-Za-z]', ' ') AS part_description,
        rcl.line_num AS line_num,
        ood.organization_code AS from_location,
        ood1.organization_code AS to_location,
        rcl.quantity_shipped AS ship_qty,
        mmt.transaction_date AS ship_date,
        rcl.quantity_received AS receive_quantity,
        rcl.last_update_date AS receive_Date,
        rcl.shipment_line_status_code AS iot_status,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || rcl.item_id AS ITEM_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || rch.organization_id AS ORGANIZATION_ID_KEY,
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
        ) || '-' || COALESCE(rcl.shipment_line_id, 0) AS dw_load_id,
        CURRENT_TIMESTAMP() AS dw_insert_date,
        CURRENT_TIMESTAMP() AS dw_update_date
      FROM silver_bec_ods.rcv_shipment_headers AS rch, silver_bec_ods.rcv_shipment_lines AS rcl, silver_bec_ods.mtl_system_items_b AS msib, silver_bec_ods.org_organization_definitions AS ood, silver_bec_ods.org_organization_definitions AS ood1, silver_bec_ods.mtl_material_transactions AS mmt
      /*	 ,xxbec_iot_number a */
      WHERE
        1 = 1
        AND rch.shipment_header_id = rcl.shipment_header_id
        AND rcl.item_id = msib.inventory_item_id
        AND rch.organization_id = msib.organization_id
        AND rcl.from_organization_id = ood.organization_id
        AND rcl.to_organization_id = ood1.organization_id
        AND rcl.mmt_transaction_id = mmt.transaction_id
        AND source_document_code = 'INVENTORY'
        AND (
          rcl.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_iot' AND batch_name = 'inv'
          )
          OR mmt.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_iot' AND batch_name = 'inv'
          )
          OR rcl.is_deleted_flg = 'Y'
          OR mmt.is_deleted_flg = 'Y'
          OR rch.is_deleted_flg = 'Y'
          OR msib.is_deleted_flg = 'Y'
          OR ood.is_deleted_flg = 'Y'
          OR ood1.is_deleted_flg = 'Y'
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
      ) || '-' || COALESCE(ods.shipment_line_id, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.FACT_INV_IOT (
  shipment_header_id,
  shipment_line_id,
  item_id,
  organization_id,
  mmt_transaction_id,
  iot,
  part_number,
  part_description,
  line_num,
  from_location,
  to_location,
  ship_qty,
  ship_date,
  receive_quantity,
  receive_date,
  iot_status,
  item_id_key,
  organization_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    rch.shipment_header_id,
    rcl.shipment_line_id,
    rcl.item_id,
    rch.organization_id,
    rcl.mmt_transaction_id,
    rch.shipment_num AS iot, /*	 ,a.rid rid */
    msib.segment1 AS part_number,
    REGEXP_REPLACE(msib.description, '[^0-9A-Za-z]', ' ') AS part_description,
    rcl.line_num AS line_num,
    ood.organization_code AS from_location,
    ood1.organization_code AS to_location,
    rcl.quantity_shipped AS ship_qty,
    mmt.transaction_date AS ship_date,
    rcl.quantity_received AS receive_quantity,
    rcl.last_update_date AS receive_Date,
    rcl.shipment_line_status_code AS iot_status,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rcl.item_id AS ITEM_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rch.organization_id AS ORGANIZATION_ID_KEY,
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
    ) || '-' || COALESCE(rcl.shipment_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.rcv_shipment_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rch, (
    SELECT
      *
    FROM silver_bec_ods.rcv_shipment_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rcl, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msib, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood1, (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt
  /*	 ,xxbec_iot_number a */
  WHERE
    1 = 1
    AND rch.shipment_header_id = rcl.shipment_header_id
    AND rcl.item_id = msib.inventory_item_id
    AND rch.organization_id = msib.organization_id
    AND rcl.from_organization_id = ood.organization_id
    AND rcl.to_organization_id = ood1.organization_id
    AND rcl.mmt_transaction_id = mmt.transaction_id
    AND source_document_code = 'INVENTORY'
    AND (
      rcl.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_iot' AND batch_name = 'inv'
      )
      OR mmt.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_iot' AND batch_name = 'inv'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_iot' AND batch_name = 'inv';