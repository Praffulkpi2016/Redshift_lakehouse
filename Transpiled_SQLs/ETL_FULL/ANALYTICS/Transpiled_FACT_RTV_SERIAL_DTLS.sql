DROP table IF EXISTS gold_bec_dwh.fact_rtv_serial_dtls;
CREATE TABLE gold_bec_dwh.fact_rtv_serial_dtls AS
(
  SELECT
    A.transaction_type_id,
    A.inventory_item_id,
    A.organization_id,
    A.po_line_location_id,
    A.po_distribution_id,
    A.SHIPMENT_HEADER_ID,
    A.transaction_id,
    A.invoice_num,
    A.receipt_number,
    A.OLD_CONSUMPTION_PO,
    A.OLD_USE_INVOICE,
    A.NEW_CONSUMPTION_PO,
    A.NEW_USE_INVOICE,
    A.part_number,
    A.description,
    A.serial_number,
    A.transaction_date,
    A.transaction_cost,
    A.shipment_number,
    A.rcv_transaction_id,
    A.current_subinventory_code,
    A.current_locator_id,
    (
      SELECT
        'PO:-' || poh.segment1 || '; LINE:-' || pol.line_num || '; REL:-' || COALESCE(por.release_num, '0') || '; CVMI FLAG:-' || COALESCE(poll.consigned_flag, 'N')
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poh, (
        SELECT
          *
        FROM silver_bec_ods.po_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pol, (
        SELECT
          *
        FROM silver_bec_ods.po_releases_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS por, (
        SELECT
          *
        FROM silver_bec_ods.po_line_locations_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poll
      WHERE
        poll.po_header_id = poh.po_header_id
        AND poll.po_line_id = pol.po_line_id
        AND poll.po_release_id = por.PO_RELEASE_ID()
        AND poll.line_location_id = a.po_line_location_id
    ) AS po_details,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.transaction_type_id AS transaction_type_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.inventory_item_id AS inventory_item_id_KEY,
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
    ) || '-' || COALESCE(A.transaction_id, 0) || '-' || COALESCE(A.ORGANIZATION_ID, 0) || '-' || COALESCE(A.inventory_item_id, 0) || '-' || COALESCE(A.SERIAL_NUMBER, 'NA') || '-' || COALESCE(A.transaction_type_id, 0) || '-' || COALESCE(A.PO_distribution_id, 0) || '-' || COALESCE(A.shipment_header_id, 0) || '-' || COALESCE(A.current_locator_id, 0) || '-' || COALESCE(A.transaction_date, CURRENT_TIMESTAMP()) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    (
      WITH ap_invoice_cnt AS (
        SELECT
          APIL.PO_DISTRIBUTION_ID,
          MIN(invoice_num) || ';' || MAX(invoice_num) || ' ; ' || COUNT(DISTINCT invoice_num) AS invoice_dtls
        FROM (
          SELECT
            *
          FROM silver_bec_ods.ap_invoices_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS api, (
          SELECT
            *
          FROM silver_bec_ods.ap_invoice_lines_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS apil
        WHERE
          api.invoice_id = apil.invoice_id
        GROUP BY
          APIL.PO_DISTRIBUTION_ID
      ), po_consumption AS (
        SELECT
          mmt.inventory_item_id,
          mut.serial_number,
          MIN(poh.segment1 || '-' || por.release_num) AS OLD_CONSUMPTION_PO,
          MAX(poh.segment1 || '-' || por.release_num) AS NEW_CONSUMPTION_PO
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mmt, (
          SELECT
            *
          FROM silver_bec_ods.mtl_unit_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mut, (
          SELECT
            *
          FROM silver_bec_ods.mtl_consumption_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mct, (
          SELECT
            *
          FROM silver_bec_ods.po_headers_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS poh, (
          SELECT
            *
          FROM silver_bec_ods.po_releases_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS por
        WHERE
          mmt.transaction_id = mut.transaction_id
          AND mmt.transaction_type_id = 74
          AND mmt.transaction_id = mct.transaction_id
          AND mct.consumption_release_id = por.po_release_id
          AND por.po_header_id = poh.po_header_id
        GROUP BY
          mmt.inventory_item_id,
          mut.serial_number
      ), use_invoice AS (
        SELECT
          mmt.inventory_item_id,
          mut.serial_number,
          MIN(api.invoice_num) AS OLD_USE_INVOICE,
          MAX(api.invoice_num) AS NEW_USE_INVOICE
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mmt, (
          SELECT
            *
          FROM silver_bec_ods.mtl_unit_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mut, (
          SELECT
            *
          FROM silver_bec_ods.mtl_consumption_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mct, (
          SELECT
            *
          FROM silver_bec_ods.po_headers_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS poh, (
          SELECT
            *
          FROM silver_bec_ods.po_releases_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS por, (
          SELECT
            *
          FROM silver_bec_ods.ap_invoices_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS api, (
          SELECT
            *
          FROM silver_bec_ods.ap_invoice_lines_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS apil
        WHERE
          mmt.transaction_id = mut.transaction_id
          AND mmt.transaction_type_id = 74
          AND mmt.transaction_id = mct.transaction_id
          AND mct.consumption_release_id = por.po_release_id
          AND por.po_header_id = poh.po_header_id
          AND mct.po_distribution_id = apil.po_distribution_id
          AND api.invoice_id = apil.invoice_id
        GROUP BY
          mmt.inventory_item_id,
          mut.serial_number
      )
      SELECT DISTINCT
        msi.inventory_item_id,
        mmt.organization_id,
        mmt.transaction_type_id,
        rct.po_line_location_id,
        rct.po_distribution_id,
        RCT.SHIPMENT_HEADER_ID,
        rct.transactioN_id,
        invct.invoice_dtls AS invoice_num,
        rsh.RECEIPT_NUM AS receipt_number,
        poc.OLD_CONSUMPTION_PO,
        inv_use.OLD_USE_INVOICE,
        poc.NEW_CONSUMPTION_PO,
        inv_use.NEW_USE_INVOICE,
        msi.segment1 AS part_number,
        msi.description,
        mut.serial_number,
        mmt.transaction_date,
        mmt.transaction_cost,
        mmt.shipment_number,
        mmt.rcv_transaction_id,
        msn.current_subinventory_code,
        msn.current_locator_id
      /* Can be replaced with FACT_PO_DETAILS by adding release num to this fact. */
      FROM (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.mtl_unit_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mut
        ON mmt.transaction_id = mut.transaction_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi
        ON MMT.INVENTORY_ITEM_ID = msi.inventory_item_id
        AND mmt.organizatioN_id = msi.organization_id
        AND NOT msi.segment1 LIKE '%DMG%'
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.rcv_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS rct
        ON mmt.rcv_transaction_id = rct.transactioN_id
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.rcv_shipment_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS RSH
        ON RCT.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.mtl_serial_numbers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msn
        ON MMT.INVENTORY_ITEM_ID = msn.inventory_item_id
        AND mut.serial_number = msn.serial_number
      LEFT JOIN ap_invoice_cnt AS invct
        ON rct.po_distribution_id = invct.po_distribution_id
      LEFT JOIN po_consumption AS poc
        ON mut.serial_number = poc.serial_number
        AND mmt.inventory_item_id = poc.inventory_item_id
      LEFT JOIN use_invoice AS inv_use
        ON mut.serial_number = inv_use.SERIAL_NUMBER()
        AND mmt.inventory_item_id = inv_use.inventory_item_id
      WHERE
        mmt.transaction_type_id = 18 /* AND mut.serial_number = 'STD134' */
      UNION ALL
      SELECT DISTINCT
        msi.inventory_item_id,
        mmt.organization_id,
        mmt.transaction_type_id,
        rct.po_line_location_id,
        rct.po_distribution_id,
        RCT.SHIPMENT_HEADER_ID,
        rct.transactioN_id,
        invct.invoice_dtls AS invoice_num,
        rsh.RECEIPT_NUM AS receipt_number,
        poc.OLD_CONSUMPTION_PO,
        inv_use.OLD_USE_INVOICE,
        poc.NEW_CONSUMPTION_PO,
        inv_use.NEW_USE_INVOICE,
        msi.segment1 AS part_number,
        msi.description,
        mut.serial_number,
        mmt.transaction_date,
        mmt.transaction_cost,
        mmt.shipment_number,
        rcv_transaction_id,
        msn.current_subinventory_code,
        msn.current_locator_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.mtl_unit_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mut
        ON mmt.transaction_id = mut.transaction_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi
        ON MMT.INVENTORY_ITEM_ID = msi.inventory_item_id
        AND mmt.organizatioN_id = msi.organization_id
        AND msi.segment1 LIKE '%DMG%'
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.rcv_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS rct
        ON mmt.rcv_transaction_id = rct.transactioN_id
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.rcv_shipment_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS RSH
        ON RCT.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.mtl_serial_numbers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msn
        ON MMT.INVENTORY_ITEM_ID = msn.inventory_item_id
        AND mut.serial_number = msn.serial_number
      LEFT JOIN ap_invoice_cnt AS invct
        ON rct.po_distribution_id = invct.po_distribution_id
      LEFT JOIN po_consumption AS poc
        ON mut.serial_number = poc.serial_number
        AND mmt.inventory_item_id = poc.inventory_item_id
      LEFT JOIN use_invoice AS inv_use
        ON mut.serial_number = inv_use.SERIAL_NUMBER()
        AND mmt.inventory_item_id = inv_use.inventory_item_id
      WHERE
        mmt.transaction_type_id <> 18
    ) /* AND item_segments = '130676'   and demand_date = to_date('2023-03-31','YYYY-MM-DD') */ /* AND  A.PLAN_ID = 40029 */
  ) AS A
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rtv_serial_dtls' AND batch_name = 'po';