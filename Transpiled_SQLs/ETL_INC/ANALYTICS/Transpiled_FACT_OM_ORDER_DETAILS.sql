DELETE FROM gold_bec_dwh.FACT_OM_ORDER_DETAILS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.OE_ORDER_LINES_ALL AS OOL, silver_bec_ods.OE_ORDER_HEADERS_ALL AS OOH, gold_bec_dwh.DIM_CUSTOMER_DETAILS AS BILL, gold_bec_dwh.DIM_CUSTOMER_DETAILS AS SHIP, gold_bec_dwh.DIM_CUSTOMER_DETAILS AS LINE_SHIP, silver_bec_ods.WSH_DELIVERY_DETAILS AS WDD, silver_bec_ods.HZ_CUST_ACCOUNTS AS HCA, silver_bec_ods.HZ_PARTIES AS HP
    WHERE
      1 = 1
      AND OOH.HEADER_ID = OOL.HEADER_ID
      AND OOH.INVOICE_TO_ORG_ID = BILL.SITE_USE_ID()
      AND BILL.SITE_USE_CODE() = 'BILL_TO'
      AND OOH.SHIP_TO_ORG_ID = SHIP.SITE_USE_ID()
      AND SHIP.SITE_USE_CODE() = 'SHIP_TO'
      AND OOL.SHIP_TO_ORG_ID = LINE_SHIP.SITE_USE_ID()
      AND LINE_SHIP.SITE_USE_CODE() = 'SHIP_TO'
      AND OOH.SOLD_TO_ORG_ID = HCA.CUST_ACCOUNT_ID()
      AND HCA.PARTY_ID = HP.PARTY_ID()
      AND OOL.HEADER_ID = WDD.SOURCE_HEADER_ID()
      AND OOL.LINE_ID = WDD.SOURCE_LINE_ID()
      AND (
        OOH.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_order_details' AND batch_name = 'om'
        )
        OR OOL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_order_details' AND batch_name = 'om'
        )
      )
      AND fact_om_order_details.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(OOH.HEADER_ID, 0) || '-' || COALESCE(OOL.LINE_ID, 0) || '-' || COALESCE(OOH.ORG_ID, 0)
  );
INSERT INTO gold_bec_dwh.fact_om_order_details
(
  SELECT
    invoice_to_org_id,
    ship_to_org_id,
    sold_to_org_id,
    org_id,
    header_id,
    order_number,
    cust_po_number,
    customer_name,
    customer_number,
    order_type_id,
    line_price_list_id,
    line_id,
    line_number,
    currency,
    conversion_rate,
    header_status_code,
    line_status_code,
    order_source_id,
    order_source_reference,
    ordered_date,
    ordered_quantity,
    book_amount,
    bill_to_customer_name,
    bill_to_customer_number,
    ship_to_customer_name,
    ship_to_customer_number,
    ship_to_addr_line1,
    ship_to_addr_line2,
    ship_to_addr_line5,
    city,
    county,
    `state`,
    country,
    context_value,
    contract_number,
    contract_description,
    gross_contract_value,
    contract_milestones_amount,
    contract_start_date,
    contract_end_date,
    header_dff7,
    account_executive,
    purchase_type,
    `deposit%`,
    project_costing_type,
    shipping_terms,
    freight_amount,
    payment_term_id,
    ordered_item,
    quantity,
    allocated_quantity,
    picked_quantity,
    open_quantity,
    unit_selling_price,
    unit_list_price,
    extended_price,
    pricing_date,
    order_quantity_uom,
    warehouse_id,
    source_type,
    line_type_id,
    line_ship_addr1,
    line_ship_addr2,
    line_ship_addr5,
    salesrep_id,
    line_category_code,
    booked_flag,
    booked_month,
    booked_year,
    ool_inventory_item_id,
    open_flag,
    booked_date,
    agreement_id,
    request_date,
    promise_date,
    accounting_rule_id,
    actual_shipment_date,
    cancelled_flag,
    created_by,
    last_updated_by,
    header_creation_date,
    line_creation_date,
    line_cust_po_number,
    deliver_to_org_id,
    fulfilled_flag,
    shippable_flag,
    fulfillment_date,
    intmed_ship_to_org_id,
    invoicing_rule_id,
    item_type_code,
    last_update_date,
    return_reason_code,
    schedule_arrival_date,
    schedule_ship_date,
    ool_ship_to_org_id,
    sales_channel_code,
    global_selling_price,
    global_list_price,
    book_quantity,
    cancel_quantity,
    ship_quantity,
    invoiced_quantity,
    cancel_amount,
    ship_amount,
    invoiced_amount,
    met_crd_amount,
    met_prd_amount,
    discount_amount,
    TAX_VALUE,
    schedule_status_code,
    BILL_TO_LOCATION,
    sales_order_line_type,
    BOOK_AMOUNT - (
      ship_amount
    ) AS ship_backlog_amount,
    ship_amount - invoiced_amount AS fin_backlog_amount,
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
    ) || '-' || COALESCE(HEADER_ID, 0) || '-' || COALESCE(LINE_ID, 0) || '-' || COALESCE(ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      OOH.INVOICE_TO_ORG_ID,
      OOH.SHIP_TO_ORG_ID,
      OOH.SOLD_TO_ORG_ID,
      OOH.ORG_ID,
      OOH.HEADER_ID,
      OOH.ORDER_NUMBER,
      OOH.CUST_PO_NUMBER,
      hp.party_name AS CUSTOMER_NAME,
      HCA.ACCOUNT_NUMBER AS CUSTOMER_NUMBER,
      OOH.ORDER_TYPE_ID,
      OOL.price_list_id AS LINE_PRICE_LIST_ID,
      OOL.LINE_ID,
      OOL.LINE_NUMBER || '.' || SHIPMENT_NUMBER AS LINE_NUMBER,
      OOH.TRANSACTIONAL_CURR_CODE AS CURRENCY,
      OOH.CONVERSION_RATE,
      OOH.FLOW_STATUS_CODE AS HEADER_STATUS_CODE,
      OOL.FLOW_STATUS_CODE AS LINE_STATUS_CODE,
      OOH.ORDER_SOURCE_ID, /* Need to get order source and description from Dimension table */
      OOH.ORIG_SYS_DOCUMENT_REF AS ORDER_SOURCE_REFERENCE,
      TRUNC(OOH.ordered_date) AS ORDERED_DATE,
      OOL.ORDERED_QUANTITY,
      (
        COALESCE(OOL.ordered_quantity, 0) * COALESCE(OOL.unit_selling_price, 0) + COALESCE(OOL.TAX_VALUE, 0)
      ) AS BOOK_AMOUNT,
      BILL.PARTY_NAME AS BILL_TO_CUSTOMER_NAME,
      BILL.ACCOUNT_NUMBER AS BILL_TO_CUSTOMER_NUMBER,
      BILL.LOCATION AS BILL_TO_LOCATION,
      ool.attribute4 AS sales_order_line_type,
      SHIP.PARTY_NAME AS SHIP_TO_CUSTOMER_NAME,
      SHIP.ACCOUNT_NUMBER AS SHIP_TO_CUSTOMER_NUMBER,
      SHIP.ADDRESS1 AS SHIP_TO_ADDR_LINE1,
      SHIP.ADDRESS2 AS SHIP_TO_ADDR_LINE2,
      SHIP.ADDRESS5 AS SHIP_TO_ADDR_LINE5,
      SHIP.CITY,
      SHIP.COUNTY,
      UPPER(SHIP.STATE) AS STATE,
      SHIP.COUNTRY, /* Attributes */
      OOH.CONTEXT AS CONTEXT_VALUE,
      OOH.ATTRIBUTE1 AS CONTRACT_NUMBER,
      OOH.ATTRIBUTE2 AS CONTRACT_DESCRIPTION,
      OOH.ATTRIBUTE3 AS GROSS_CONTRACT_VALUE,
      OOH.ATTRIBUTE4 AS CONTRACT_MILESTONES_AMOUNT,
      OOH.ATTRIBUTE5 AS CONTRACT_START_DATE,
      OOH.ATTRIBUTE6 AS CONTRACT_END_DATE,
      OOH.ATTRIBUTE7 AS HEADER_DFF7,
      CASE
        WHEN CASE WHEN OOH.attribute8 = ' India' THEN 'INDIA' ELSE UPPER(OOH.ATTRIBUTE8) END = 'INDIA'
        THEN '999999'
        ELSE OOH.ATTRIBUTE8
      END AS ACCOUNT_EXECUTIVE,
      OOH.ATTRIBUTE9 AS PURCHASE_TYPE,
      OOH.ATTRIBUTE10 AS `DEPOSIT%`,
      OOH.ATTRIBUTE11 AS PROJECT_COSTING_TYPE,
      OOH.ATTRIBUTE12 AS SHIPPING_TERMS,
      OOH.ATTRIBUTE14 AS FREIGHT_AMOUNT, /* ,OOL.LINE_TYPE_ID */
      OOH.PAYMENT_TERM_ID, /* Need to get payment term from dimension table. */ /* ,OOH.PRICE_LIST_ID HDR_PRICE_LIST_ID */
      OOL.ORDERED_ITEM, /* ,MSI.DESCRIPTION ITEM_DESCRIPTION --Get item desc from dim_inv_master_items */
      OOL.ORDERED_QUANTITY AS QUANTITY,
      SUM(CASE WHEN wdd.released_status = 'S' THEN wdd.PICKED_QUANTITY END) AS ALLOCATED_QUANTITY,
      SUM(CASE WHEN wdd.released_status = 'Y' THEN wdd.PICKED_QUANTITY END) AS PICKED_QUANTITY,
      OOL.ORDERED_QUANTITY AS OPEN_QUANTITY,
      COALESCE(OOL.unit_selling_price, 0) AS UNIT_SELLING_PRICE,
      COALESCE(OOL.unit_list_price, 0) AS UNIT_LIST_PRICE,
      (
        COALESCE(OOL.ORDERED_QUANTITY, 0) * COALESCE(OOL.UNIT_SELLING_PRICE, 0)
      ) AS EXTENDED_PRICE,
      OOL.PRICING_DATE,
      OOL.ORDER_QUANTITY_UOM, /* ,OOL.CREATION_DATE */ /* ,OOL.REQUEST_DATE */ /* ,OOL.SCHEDULE_SHIP_DATE */
      OOL.SHIP_FROM_ORG_ID AS WAREHOUSE_ID, /* Need to get name from Dimension table. */
      OOL.SOURCE_TYPE_CODE AS SOURCE_TYPE, /* Need to get meaning from dimension table where LOOKUP_TYPE = 'SOURCE_TYPE' and lookup code = source type. */
      OOL.LINE_TYPE_ID, /* Need to get line type from Dimension table. */
      LINE_SHIP.ADDRESS1 AS LINE_SHIP_ADDR1,
      LINE_SHIP.ADDRESS2 AS LINE_SHIP_ADDR2,
      LINE_SHIP.ADDRESS5 AS LINE_SHIP_ADDR5, /* Document Type and Document Number can be retrived from DIM_Lookups and DIM_OM_PO_NUM tables. */
      OOL.salesrep_id, /* Need to get NAME from DIM_OM_SALES_REPS dimension table. */
      OOL.line_category_code,
      OOL.booked_flag,
      DATE_FORMAT(OOH.booked_date, 'MON-RRRR') AS BOOKED_MONTH,
      DATE_FORMAT(OOH.booked_date, 'RRRR') AS BOOKED_YEAR,
      OOL.INVENTORY_ITEM_ID AS OOL_INVENTORY_ITEM_ID,
      OOL.open_flag AS OPEN_FLAG,
      OOH.booked_date AS BOOKED_DATE,
      OOH.agreement_id,
      TRUNC(OOL.request_date) AS REQUEST_DATE,
      TRUNC(OOL.promise_date) AS promise_date,
      OOL.accounting_rule_id,
      TRUNC(OOL.actual_shipment_date) AS actual_shipment_date,
      OOL.cancelled_flag,
      OOL.created_by,
      OOL.last_updated_by,
      TRUNC(OOh.creation_date) AS HEADER_CREATION_DATE,
      TRUNC(OOL.creation_date) AS LINE_CREATION_DATE,
      OOL.cust_po_number AS LINE_CUST_PO_NUMBER,
      OOL.deliver_to_org_id,
      OOL.fulfilled_flag,
      OOL.shippable_flag,
      TRUNC(OOL.fulfillment_date) AS FULFILLMENT_DATE,
      OOL.intmed_ship_to_org_id,
      OOL.invoicing_rule_id,
      OOL.item_type_code,
      TRUNC(OOL.last_update_date) AS LAST_UPDATE_DATE,
      OOL.return_reason_code,
      TRUNC(OOL.schedule_arrival_date) AS schedule_arrival_date,
      TRUNC(OOL.schedule_ship_date) AS schedule_ship_date,
      OOL.ship_to_org_id AS OOL_SHIP_TO_ORG_ID,
      OOH.sales_channel_code,
      ROUND(COALESCE(OOL.unit_selling_price, 0), 2) AS GLOBAL_SELLING_PRICE,
      ROUND(COALESCE(OOL.unit_list_price, 0), 2) AS GLOBAL_LIST_PRICE,
      COALESCE(OOL.ordered_quantity, 0) AS BOOK_QUANTITY,
      COALESCE(OOL.cancelled_quantity, 0) AS CANCEL_QUANTITY,
      COALESCE(OOL.shipped_quantity, 0) AS SHIP_QUANTITY,
      COALESCE(OOL.invoiced_quantity, 0) AS INVOICED_QUANTITY,
      (
        COALESCE(OOL.cancelled_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ) AS CANCEL_AMOUNT,
      (
        COALESCE(OOL.shipped_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ) AS SHIP_AMOUNT,
      (
        COALESCE(OOL.invoiced_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ) AS INVOICED_AMOUNT,
      (
        CASE
          WHEN OOL.actual_shipment_date <= OOL.request_date
          THEN (
            COALESCE(OOL.shipped_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
          )
          ELSE 0
        END
      ) AS MET_CRD_AMOUNT,
      (
        CASE
          WHEN OOL.actual_shipment_date <= OOL.promise_date
          THEN (
            COALESCE(OOL.shipped_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
          )
          ELSE 0
        END
      ) AS MET_PRD_AMOUNT,
      (
        COALESCE(OOL.unit_list_price, 0) - COALESCE(OOL.unit_selling_price, 0)
      ) * COALESCE(OOL.ordered_quantity, 0) AS discount_amount,
      COALESCE(OOL.TAX_VALUE, 0) AS TAX_VALUE, /* Added new column to fix Jira#497 */
      COALESCE(ool.schedule_status_code, 'N') AS schedule_status_code
    FROM (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_LINES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOL, (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOH, gold_bec_dwh.DIM_CUSTOMER_DETAILS AS BILL, gold_bec_dwh.DIM_CUSTOMER_DETAILS AS SHIP, gold_bec_dwh.DIM_CUSTOMER_DETAILS AS LINE_SHIP, (
      SELECT
        *
      FROM silver_bec_ods.WSH_DELIVERY_DETAILS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WDD, (
      SELECT
        *
      FROM silver_bec_ods.HZ_CUST_ACCOUNTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HCA, (
      SELECT
        *
      FROM silver_bec_ods.HZ_PARTIES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HP
    /* ,MTL_SYSTEM_ITEMS_B MSI */
    WHERE
      1 = 1
      AND OOH.HEADER_ID = OOL.HEADER_ID
      AND OOH.INVOICE_TO_ORG_ID = BILL.SITE_USE_ID()
      AND BILL.SITE_USE_CODE() = 'BILL_TO'
      AND OOH.SHIP_TO_ORG_ID = SHIP.SITE_USE_ID()
      AND SHIP.SITE_USE_CODE() = 'SHIP_TO'
      AND OOL.SHIP_TO_ORG_ID = LINE_SHIP.SITE_USE_ID()
      AND LINE_SHIP.SITE_USE_CODE() = 'SHIP_TO'
      AND OOH.SOLD_TO_ORG_ID = HCA.CUST_ACCOUNT_ID()
      AND HCA.PARTY_ID = HP.PARTY_ID()
      AND OOL.HEADER_ID = WDD.SOURCE_HEADER_ID()
      AND OOL.LINE_ID = WDD.SOURCE_LINE_ID()
      AND (
        OOH.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_order_details' AND batch_name = 'om'
        )
        OR OOL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_order_details' AND batch_name = 'om'
        )
      )
    GROUP BY
      OOH.INVOICE_TO_ORG_ID,
      OOH.SHIP_TO_ORG_ID,
      OOH.SOLD_TO_ORG_ID,
      OOH.ORG_ID,
      OOL.SHIP_FROM_ORG_ID,
      OOH.HEADER_ID,
      OOL.LINE_ID,
      OOL.LINE_NUMBER || '.' || OOL.SHIPMENT_NUMBER,
      OOH.ORDER_NUMBER,
      OOH.CUST_PO_NUMBER,
      hp.party_name,
      HCA.ACCOUNT_NUMBER,
      BILL.PARTY_NAME,
      BILL.ACCOUNT_NUMBER,
      BILL.LOCATION,
      ool.attribute4,
      SHIP.PARTY_NAME,
      SHIP.ACCOUNT_NUMBER,
      SHIP.ADDRESS1,
      SHIP.ADDRESS2,
      SHIP.ADDRESS5,
      SHIP.CITY,
      SHIP.COUNTY,
      UPPER(SHIP.STATE),
      SHIP.COUNTRY,
      OOH.ORDER_TYPE_ID,
      OOL.LINE_TYPE_ID, /* ,OOH.PRICE_LIST_ID */
      OOH.ORDER_SOURCE_ID,
      OOH.TRANSACTIONAL_CURR_CODE,
      OOH.CONVERSION_RATE,
      OOH.FLOW_STATUS_CODE, /* ,OOH.ORDERED_DATE */
      OOH.PAYMENT_TERM_ID,
      OOH.ORIG_SYS_DOCUMENT_REF,
      OOL.FLOW_STATUS_CODE,
      OOL.ORDERED_ITEM,
      OOL.ORDERED_QUANTITY,
      OOL.SOURCE_TYPE_CODE,
      LINE_SHIP.ADDRESS1,
      LINE_SHIP.ADDRESS2,
      LINE_SHIP.ADDRESS5,
      OOH.CONTEXT,
      OOH.ATTRIBUTE1,
      OOH.ATTRIBUTE2,
      OOH.ATTRIBUTE3,
      OOH.ATTRIBUTE4,
      OOH.ATTRIBUTE5,
      OOH.ATTRIBUTE6,
      OOH.ATTRIBUTE7, /* ,OOH.ATTRIBUTE8 */
      CASE
        WHEN CASE WHEN OOH.attribute8 = ' India' THEN 'INDIA' ELSE UPPER(OOH.ATTRIBUTE8) END = 'INDIA'
        THEN '999999'
        ELSE OOH.ATTRIBUTE8
      END,
      OOH.ATTRIBUTE9,
      OOH.ATTRIBUTE10,
      OOH.ATTRIBUTE11,
      OOH.ATTRIBUTE12,
      OOH.ATTRIBUTE14,
      OOL.ORDERED_QUANTITY, /* ,OOL.UNIT_SELLING_PRICE */ /* ,OOL.UNIT_LIST_PRICE */ /* ,(NVL(OOL.ORDERED_QUANTITY,0) * NVL(OOL.UNIT_SELLING_PRICE,0)) */
      (
        COALESCE(OOL.ordered_quantity, 0) * COALESCE(OOL.unit_selling_price, 0) + COALESCE(OOL.TAX_VALUE, 0)
      ),
      OOL.PRICING_DATE,
      OOL.ORDER_QUANTITY_UOM, /* ,OOL.CREATION_DATE */ /* ,OOL.REQUEST_DATE */ /* ,OOL.SCHEDULE_SHIP_DATE */
      OOL.PRICING_DATE,
      OOL.ORDER_QUANTITY_UOM,
      OOL.salesrep_id,
      OOL.line_category_code,
      OOL.booked_flag,
      DATE_FORMAT(OOH.booked_date, 'MON-RRRR'),
      DATE_FORMAT(OOH.booked_date, 'RRRR'),
      OOL.INVENTORY_ITEM_ID,
      OOL.open_flag,
      OOH.booked_date,
      OOH.agreement_id,
      TRUNC(OOH.ordered_date),
      TRUNC(OOL.request_date),
      TRUNC(OOL.promise_date),
      OOL.accounting_rule_id,
      TRUNC(OOL.actual_shipment_date),
      OOL.cancelled_flag,
      OOL.created_by,
      OOL.last_updated_by,
      TRUNC(OOh.creation_date),
      TRUNC(OOL.creation_date),
      OOL.cust_po_number,
      OOL.deliver_to_org_id,
      OOL.fulfilled_flag,
      OOL.shippable_flag,
      TRUNC(OOL.fulfillment_date),
      OOL.intmed_ship_to_org_id,
      OOL.invoicing_rule_id,
      OOL.item_type_code,
      TRUNC(OOL.last_update_date),
      OOL.price_list_id,
      OOL.return_reason_code,
      TRUNC(OOL.schedule_arrival_date),
      TRUNC(OOL.schedule_ship_date),
      OOL.ship_to_org_id,
      OOH.sales_channel_code,
      COALESCE(OOL.unit_selling_price, 0),
      ROUND(COALESCE(OOL.unit_selling_price, 0), 2),
      COALESCE(OOL.unit_list_price, 0),
      ROUND(COALESCE(OOL.unit_list_price, 0), 2),
      COALESCE(OOL.ordered_quantity, 0),
      COALESCE(OOL.cancelled_quantity, 0),
      COALESCE(OOL.shipped_quantity, 0),
      COALESCE(OOL.invoiced_quantity, 0),
      (
        COALESCE(OOL.ordered_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ),
      (
        COALESCE(OOL.cancelled_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ),
      (
        COALESCE(OOL.shipped_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ),
      (
        COALESCE(OOL.invoiced_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
      ),
      (
        CASE
          WHEN OOL.actual_shipment_date <= OOL.request_date
          THEN (
            COALESCE(OOL.shipped_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
          )
          ELSE 0
        END
      ),
      (
        CASE
          WHEN OOL.actual_shipment_date <= OOL.promise_date
          THEN (
            COALESCE(OOL.shipped_quantity, 0) * COALESCE(OOL.unit_selling_price, 0)
          )
          ELSE 0
        END
      ),
      (
        COALESCE(OOL.unit_list_price, 0) - COALESCE(OOL.unit_selling_price, 0)
      ) * COALESCE(OOL.ordered_quantity, 0),
      COALESCE(OOL.TAX_VALUE, 0),
      COALESCE(ool.schedule_status_code, 'N')
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_order_details' AND batch_name = 'om';