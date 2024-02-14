/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_OPEN_PO_DATA
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.PO_HEADERS_ALL AS poh, silver_bec_ods.PO_LINES_ALL AS pol, silver_bec_ods.MTL_SYSTEM_ITEMS_B AS msib, silver_bec_ods.PO_LINE_LOCATIONS_ALL AS poll, silver_bec_ods.PO_DISTRIBUTIONS_ALL AS pod
    WHERE
      1 = 1
      AND pol.item_id = msib.inventory_item_id
      AND poh.po_header_id = pol.po_header_id
      AND pol.po_line_id = pod.po_line_id
      AND poll.line_location_id = pod.line_location_id
      AND pOd.destination_organization_id = MSIB.organization_id
      AND (
        pod.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_open_po_data' AND batch_name = 'po'
        )
        OR poh.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_open_po_data' AND batch_name = 'po'
        )
        OR poll.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_open_po_data' AND batch_name = 'po'
        )
      )
      AND FACT_OPEN_PO_DATA.po_header_id = poh.po_header_id
      AND FACT_OPEN_PO_DATA.po_line_id = pol.po_line_id
      AND FACT_OPEN_PO_DATA.line_location_id = poll.line_location_id
      AND FACT_OPEN_PO_DATA.po_distribution_id = pod.po_distribution_id
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_OPEN_PO_DATA (
  po_header_id,
  po_number,
  po_type,
  po_status,
  closed_code,
  po_creation_date,
  po_last_update_date,
  org_id,
  po_line_id,
  line_num,
  unit_price,
  line_quantity,
  item_id,
  item,
  organization_id,
  line_location_id,
  shipment_num,
  ship_quantity,
  need_by_date,
  promised_date,
  ship_to_organization_id,
  shipment_closed_code,
  po_distribution_id,
  amount,
  destination_subinventory,
  po_header_id_key,
  org_id_key,
  po_line_id_key,
  item_id_key,
  organization_id_key,
  line_location_id_key,
  ship_to_organization_id_key,
  po_distribution_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    poh.po_header_id,
    poh.segment1 AS po_number,
    poh.type_lookup_code AS po_type,
    poh.authorization_status AS po_status,
    poh.closed_code,
    poh.creation_date AS po_creation_date,
    poh.last_update_date AS po_last_update_date,
    poh.org_id,
    pol.po_line_id,
    pol.line_num,
    pol.unit_price,
    pol.quantity AS line_quantity,
    pol.item_id,
    msib.segment1 AS item,
    msib.organization_id,
    poll.line_location_id,
    poll.shipment_num,
    poll.quantity AS ship_quantity,
    poll.need_by_date,
    poll.promised_date,
    poll.ship_to_organization_id,
    poll.closed_code AS shipment_closed_code,
    pod.po_distribution_id,
    pod.amount_billed AS amount,
    pod.destination_subinventory,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || poh.po_header_id AS po_header_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || poh.org_id AS org_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pol.po_line_id AS po_line_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pol.item_id AS item_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || msib.organization_id AS organization_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || poll.line_location_id AS line_location_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || poll.ship_to_organization_id AS ship_to_organization_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pod.po_distribution_id AS po_distribution_id_key,
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
    ) || '-' || COALESCE(poh.po_header_id, 0) || '-' || COALESCE(pol.po_line_id, 0) || '-' || COALESCE(poll.line_location_id, 0) || '-' || COALESCE(pod.po_distribution_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PO_HEADERS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS poh, (
    SELECT
      *
    FROM silver_bec_ods.PO_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pol, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msib, (
    SELECT
      *
    FROM silver_bec_ods.PO_LINE_LOCATIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS poll, (
    SELECT
      *
    FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pod
  WHERE
    1 = 1
    AND pol.item_id = msib.inventory_item_id
    AND poh.po_header_id = pol.po_header_id
    AND pol.po_line_id = pod.po_line_id
    AND poll.line_location_id = pod.line_location_id
    AND pOd.destination_organization_id = MSIB.organization_id
    AND COALESCE(poll.closed_code, 'OPEN') = 'OPEN'
    AND COALESCE(poh.closed_code, 'OPEN') = 'OPEN'
    AND (
      pod.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_open_po_data' AND batch_name = 'po'
      )
      OR poh.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_open_po_data' AND batch_name = 'po'
      )
      OR poll.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_open_po_data' AND batch_name = 'po'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_open_po_data' AND batch_name = 'po';