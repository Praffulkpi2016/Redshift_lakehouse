 /*
# COPYRIGHT(C) 2022 KPI PARTNERS, INC. ALL RIGHTS RESERVED.
#
# UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING, SOFTWARE
# DISTRIBUTED UNDER THE LICENSE IS DISTRIBUTED ON AN "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED.
#
# AUTHOR: KPI PARTNERS, INC.
# VERSION: 2022.06
# DESCRIPTION: THIS SCRIPT REPRESENTS INCREMENTAL LOAD APPROACH FOR FACTS.
# FILE VERSION: KPI V1.0
*/

BEGIN;

TRUNCATE TABLE BEC_DWH.FACT_ASN_SHIPMENT;

INSERT INTO BEC_DWH.FACT_ASN_SHIPMENT 
  (SELECT 
       SHP.SHIPMENT_HEADER_ID,
       SHP.SHIPMENT_LINE_ID,
       SHP.PO_HEADER_ID,
       SHP.PO_LINE_ID,
		SHP.PO_NUMBER,
		SHP.PO_STATUS_LOOKUP_CODE,
		SHP.ITEM_ID,
		SHP.AGENT_ID,
		SHP.PO_DATE,
		SHP.LOCATOR_ID,
		SHP.PO_LINE_QUANTITY,
		SHP.LINE_TYPE_ID,
		SHP.ITEM_CATEGORY_ID,
		SHP.ORG_ID,
		SHP.vendor_id,
		SHP.vendor_site_id,
		SHP.SHIPMENT_HEADER_ID_KEY,
        SHP.SHIPMENT_LINE_ID_KEY,
		SHP.PO_HEADER_ID_KEY,
        SHP.PO_LINE_ID_KEY,
		SHP.ITEM_ID_KEY,
		SHP.AGENT_ID_KEY,
		SHP.ORG_ID_KEY,
		SHP.vendor_id_key,
		SHP.vendor_site_id_key,
		SHP.LOCATOR_ID_key,
		SHP.ITEM_CATEGORY_ID_key,
		SHP.LINE_TYPE_ID_KEY,		
       SHP.ASN_TYPE,
       SHP.VENDOR_NAME,
       SHP.RSH_PACKING_SLIP,
       SHP.SHIPMENT_NUM,
       SHP.ASN_LINE,
       SHP.PO_SHIPMENT_LINE,
       SHP.WAYBILL_AIRBILL_NUM,
       SHP.RECEIPT_NUM,
       SHP.SHIPMENT_LINE_STATUS_CODE,
       SHP.CREATION_DATE,
       SHP.SHIPPED_DATE,
       SHP.EXPECTED_RECEIPT_DATE,
       SHP.RSH_COMMENTS,
       SHP.BILL_OF_LADING,
       SHP.FREIGHT_CARRIER_CODE,
       SHP.NUM_OF_CONTAINERS,
       SHP.PART_NUMBER,
       SHP.ITEM_DESCRIPTION,
       SHP.ITEM_REVISION,
       SHP.SOURCE_DOCUMENT_TYPE,
       SHP.ORDER_NUM,
       SHP.ORDER_LINE_NUM,
       SHP.LINE_NUM,
       SHP.RELEASE_NUM,
       SHP.NEED_BY_DATE,
       SHP.QUANTITY_SHIPPED,
       SHP.QUANTITY_RECEIVED,
       SHP.UNIT_OF_MEASURE,
       SHP.PRIMARY_UNIT_OF_MEASURE,
       SHP.ROUTING_NAME,
       SHP.RECEIPT_SOURCE_CODE,
       SHP.TO_ORGANIZATION_ID,
       SHP.ORGANIZATION_NAME,
       SHP.CONTAINER_NUM,
       SHP.LOT_NUM,
       SHP.SERIAL_NUM,
       SHP.LOT_QTY,
       SHP.SERIAL_NUMBER_CONTROL,
       SHP.CURRENT_STATUS,
       SHP.LOT_CONTROL,
       SHP.PAY_ON_CODE,
       SHP.TERM_NAME,
       SHP.UNIT_PRICE,
       SHP.SHIPMENT_AMOUNT,
        SHP.PROMISED_DATE,
        SHP.COMMIT_DATE,
		SHP.LAST_UPDATE_DATE,
		SHP.CVMI_FLAG,
		SHP.ITEM_COST,
		SHP.MATERIAL_COST,
		SHP.PO_DESCRIPTION,
		SHP.AUTHORIZATION_STATUS,
		SHP.PO_UNIT_PRICE,
		SHP.QUANTITY_ORDERED,
		SHP.VMI_FLAG,
		SHP.REQUISITION_NUMBER,
		SHP.CHARGE_ACCOUNT_ID,
		SHP.TO_SUBINVENTORY,
		SHP.DELIVER_TO_PERSON,
		SHP.type_lookup_code,
		(SELECT mc.segment1 || '.' || mc.segment2 
             FROM bec_ods.mtl_categories_b mc,
                  bec_ods.mtl_item_categories mic
            WHERE    mic.inventory_item_id = SHP.ITEM_ID
                  AND mic.organization_id  = SHP.TO_ORGANIZATION_ID
                  AND mic.category_id      = mc.category_id
                  AND mic.category_set_id  = 1) CATEGORY,
		DECODE(SHP.SERIAL_NUM,NULL,NULL,1) SERIAL_NUM_QTY,
		substring (SHP.TERM_NAME, 1, 3) payment_term,
		PAYMENT_dAYS,
		--Payment_date+PAYMENT_dAYS as payment_due_date,
		dateadd(Day,PAYMENT_dAYS,Payment_date) payment_due_date,
	    shp.wip_entity_id,
		'N' AS IS_DELETED_FLG,
		SHP.SOURCE_APP_ID,
		SHP.DW_LOAD_ID, 
		SHP.DW_INSERT_DATE,
		SHP.DW_UPDATE_DATE	
  
 FROM 
	(     SELECT STG.SHIPMENT_HEADER_ID,
        STG.SHIPMENT_LINE_ID,
        STG.PO_HEADER_ID,
        POL.PO_LINE_ID,
		POH.SEGMENT1 PO_NUMBER,
		POH.STATUS_LOOKUP_CODE PO_STATUS_LOOKUP_CODE,
		STG.ITEM_ID,
		POH.AGENT_ID,
		POH.CREATION_DATE PO_DATE,
		STG.LOCATOR_ID,
		STG.ORG_ID,
		STG.vendor_id,
		POH.vendor_site_id,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' || STG.SHIPMENT_HEADER_ID  SHIPMENT_HEADER_ID_KEY,
        (SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' || STG.SHIPMENT_LINE_ID    SHIPMENT_LINE_ID_KEY,
        (SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' || STG.PO_HEADER_ID        PO_HEADER_ID_KEY,
        (SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||POL.PO_LINE_ID           PO_LINE_ID_KEY,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||STG.ITEM_ID              ITEM_ID_KEY,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||POH.AGENT_ID 			 AGENT_ID_KEY,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||STG.ORG_ID 				 ORG_ID_KEY,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||STG.vendor_id 			 vendor_id_key,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||POH.vendor_site_id 		 vendor_site_id_key,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||STG.LOCATOR_ID 			 LOCATOR_ID_key,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||STG.ITEM_CATEGORY_ID 	 ITEM_CATEGORY_ID_key,
		(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID WHERE SOURCE_SYSTEM = 'EBS')|| '-' ||POL.LINE_TYPE_ID 		 LINE_TYPE_ID_KEY,
		POL.QUANTITY PO_LINE_QUANTITY,
		POL.LINE_TYPE_ID,
		STG.ITEM_CATEGORY_ID,		
        STG.ASN_TYPE,
        STG.VENDOR_NAME,
        STG.RSH_PACKING_SLIP,
        STG.SHIPMENT_NUM,
        STG.LINE_NUM           ASN_LINE,
        POLL.SHIPMENT_NUM      PO_SHIPMENT_LINE,
        STG.WAYBILL_AIRBILL_NUM,
        STG.RECEIPT_NUM,
        STG.SHIPMENT_LINE_STATUS_CODE,
        STG.CREATION_DATE,
        STG.SHIPPED_DATE,
        STG.EXPECTED_RECEIPT_DATE,
        STG.RSH_COMMENTS,
        STG.BILL_OF_LADING,
        STG.FREIGHT_CARRIER_CODE,
        STG.NUM_OF_CONTAINERS,
        MSI.SEGMENT1           PART_NUMBER,
        STG.ITEM_DESCRIPTION,
        STG.ITEM_REVISION,
        STG.SOURCE_DOCUMENT_TYPE,
        STG.ORDER_NUM,
        STG.ORDER_LINE_NUM,
        STG.LINE_NUM,
        STG.RELEASE_NUM,
        STG.NEED_BY_DATE,
        STG.QUANTITY_SHIPPED   QUANTITY_SHIPPED,
        STG.QUANTITY_RECEIVED,
        STG.UNIT_OF_MEASURE,
        STG.PRIMARY_UNIT_OF_MEASURE,
        STG.ROUTING_NAME,
        STG.RECEIPT_SOURCE_CODE,
        STG.TO_ORGANIZATION_ID,
        OOD.ORGANIZATION_NAME,
        NVL((STG.ORDER_NUM), '0')
        || '('
        || NVL((STG.RELEASE_NUM), '0')
        || ')('
        || NVL((STG.LINE_NUM), '0')
        || ')('
        || NVL((POLL.SHIPMENT_NUM), '0')
        || ')' ORDER_TEXT,
        NVL(STG.CONTAINER_NUM, STG.RSH_COMMENTS) CONTAINER_NUM,
        RCVLS.LOT_NUM,
        RSS.SERIAL_NUM,
        RCVLS.QUANTITY         LOT_QTY,
        DECODE(MSI.SERIAL_NUMBER_CONTROL_CODE, 5, 'AT RECEIPT', 1, 'NO CONTROL') SERIAL_NUMBER_CONTROL,
        (
            SELECT
                DECODE(CURRENT_STATUS, 1, 'DEFINED BUT NOT USED', 3, 'RESIDES IN STORES',
                       4, 'ISSUED OUT OF STORES', 5, 'RESIDES IN INTRANSIT', 7,
                       'RESIDES IN RECEIVING', 8, 'RESIDES IN WIP') CURRENT_STATUS
            FROM
                BEC_ODS.MTL_SERIAL_NUMBERS MSN
            WHERE
                STG.ITEM_ID = MSN.INVENTORY_ITEM_ID
                AND STG.TO_ORGANIZATION_ID = MSN.CURRENT_ORGANIZATION_ID
                AND RSS.SERIAL_NUM = MSN.SERIAL_NUMBER
        ) CURRENT_STATUS,
        DECODE(MSI.LOT_CONTROL_CODE, 1, 'NO CONTROL', 2, 'FULL CONTROL') LOT_CONTROL,
        POH.PAY_ON_CODE,
        AT.NAME                TERM_NAME,
        POL.UNIT_PRICE,
        DECODE(RSS.SERIAL_NUM, NULL,(POL.UNIT_PRICE * STG.QUANTITY_SHIPPED),(POL.UNIT_PRICE * 1)) SHIPMENT_AMOUNT,
		DECODE (
                             substring (AT.NAME, 1, 3),
                             'NET', substring (AT.NAME, 4, LENGTH (AT.NAME))::INT,
                             0)::INT  PAYMENT_dAYS,
		DECODE(POH.PAY_ON_CODE, 'RECEIPT', STG.EXPECTED_RECEIPT_DATE,STG.SHIPPED_DATE) Payment_date, 
     --   DECODE(POH.PAY_ON_CODE, 'RECEIPT', TO_DATE(STG.EXPECTED_RECEIPT_DATE, 'DD/MM/YYYY') + DECODE(SUBSTR(AT.NAME, 1, 3), 'NET'
     --   , SUBSTR(AT.NAME, 4, LENGTH(AT.NAME)), 0), TO_DATE(STG.SHIPPED_DATE, 'DD/MM/YYYY') + DECODE(SUBSTR(AT.NAME, 1, 3), 'NET',
     --   SUBSTR(AT.NAME, 4, LENGTH(AT.NAME)), 0)) PAYMENT_DUE_DATE,
          --CAT.CATEGORY,
          --WIP."SEGMENT1",
          --WIP."WIP_ENTITY_NAME",
          --WIP."WO_DESCRIPTION",
          --WIP."WO_PART_NO",
          --WIP."WO_PART_DESCRIPTION",
          --WIP."PO_HEADER_ID",
        POLL.PROMISED_DATE,
        POLL.ATTRIBUTE10 COMMIT_DATE,
		STG.LAST_UPDATE_DATE,
		nvl(POLL.CONSIGNED_FLAG, 'N') CVMI_FLAG,
		CST.ITEM_COST,
		CST.MATERIAL_COST,
		POH.COMMENTS PO_DESCRIPTION,
		POH.AUTHORIZATION_STATUS,
		POL.UNIT_PRICE PO_UNIT_PRICE,
		POLL.QUANTITY QUANTITY_ORDERED,
		nvl(poll.VMI_FLAG,'N') VMI_FLAG,
		REQ.REQUISITION_NUMBER,
		STG.CHARGE_ACCOUNT_ID,
		STG.TO_SUBINVENTORY,
		STG.DELIVER_TO_PERSON,
		Poh.type_lookup_code,
		req.wip_entity_id,
        (
			SELECT
				SYSTEM_ID
			FROM
				BEC_ETL_CTRL.ETLSOURCEAPPID
			WHERE
				SOURCE_SYSTEM = 'EBS'
			) AS SOURCE_APP_ID,
			(
			SELECT
				SYSTEM_ID
			FROM
				BEC_ETL_CTRL.ETLSOURCEAPPID
			WHERE
				SOURCE_SYSTEM = 'EBS'
			)
			 --  || '-' || NVL(STG.VENDOR_ID, 0)
			   || '-' || NVL(STG.PO_HEADER_ID, 0)
			   || '-' || NVL(POL.PO_LINE_ID, 0) 
			   || '-' || NVL(RCVLS.LOT_NUM, '0')	
			   || '-' || NVL(STG.SHIPMENT_HEADER_ID, 0) 
			   || '-' || NVL(STG.SHIPMENT_LINE_ID, '0')	  
			   || '-' || NVL(REQ.REQUISITION_NUMBER, '0')	
			   || '-' || NVL(RSS.SERIAL_NUM, 'NA') AS DW_LOAD_ID, 
			GETDATE() AS DW_INSERT_DATE,
			GETDATE() AS DW_UPDATE_DATE			
		from
			(select * from BEC_DWH.FACT_PO_SHIPMENT_STG where is_deleted_flg <> 'Y') STG,
			(select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg <> 'Y') MSI,
			(select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg <> 'Y') POLL,
			(select * from bec_ods.ORG_ORGANIZATION_DEFINITIONS where is_deleted_flg <> 'Y') OOD,
			(select * from bec_ods.RCV_SERIALS_SUPPLY where is_deleted_flg <> 'Y') RSS,
			(select * from bec_ods.RCV_LOTS_SUPPLY where is_deleted_flg <> 'Y') RCVLS,
			(select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg <> 'Y') POH,
			(select * from bec_ods.AP_TERMS where is_deleted_flg <> 'Y') at,
			(select * from bec_ods.PO_LINES_ALL where is_deleted_flg <> 'Y') POL,
			(
			select
				distinct prh.segment1 REQUISITION_NUMBER,
				POD.PO_HEADER_ID,
				POD.PO_LINE_ID,prl.wip_entity_id
			from
				(select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y') prh,
				(select * from bec_ods.po_requisition_lines_all where is_deleted_flg <> 'Y') prl,
				(select * from bec_ods.po_req_distributions_all where is_deleted_flg <> 'Y') prd,
				(select * from bec_ods.po_distributions_all where is_deleted_flg <> 'Y') pod
			where
				1 = 1
				and prh.interface_source_code = 'WIP'
				and prh.requisition_header_id = prl.requisition_header_id
				and prl.requisition_line_id = prd.requisition_line_id
				and prd.distribution_id = pod.req_distribution_id) REQ,
			(select * from bec_ods.CST_ITEM_COSTS where is_deleted_flg <> 'Y') CST
		where
			1 = 1
			and STG.RECEIPT_SOURCE_CODE = 'VENDOR'
			and STG.PO_HEADER_ID = POH.PO_HEADER_ID
			--AND STG.PO_HEADER_ID = WIP.PO_HEADER_ID (+)
			and POH.TERMS_ID = AT.TERM_ID(+)
			and POH.PO_HEADER_ID = POL.PO_HEADER_ID
			and POH.PO_HEADER_ID = POLL.PO_HEADER_ID
			and POL.PO_LINE_ID = POLL.PO_LINE_ID
			and NVL(STG.QUANTITY_SHIPPED, 0) <> NVL(STG.QUANTITY_RECEIVED, 0)
				and STG.ITEM_ID = MSI.INVENTORY_ITEM_ID (+)
				and STG.TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID (+)
				and STG.PO_LINE_LOCATION_ID = POLL.LINE_LOCATION_ID (+)
				and STG.SHIPMENT_LINE_ID = RSS.SHIPMENT_LINE_ID (+)
				and STG.SHIPMENT_LINE_ID = RCVLS.SHIPMENT_LINE_ID (+)
				and STG.SHIPMENT_LINE_STATUS_CODE <> 'FULLY RECEIVED'
				and STG.TO_ORGANIZATION_ID = OOD.ORGANIZATION_ID
				--AND STG.ITEM_ID = CAT.INVENTORY_ITEM_ID(+)
				--AND STG.TO_ORGANIZATION_ID = CAT.ORGANIZATION_ID(+);
				and MSI.INVENTORY_ITEM_ID = CST.INVENTORY_ITEM_ID(+)
				and MSI.ORGANIZATION_ID = CST.ORGANIZATION_ID(+)
				and POLL.PO_HEADER_ID = REQ.PO_HEADER_ID (+)
				and POLL.PO_LINE_ID = REQ.PO_LINE_ID (+)
				and CST.ORGANIZATION_ID(+) = 1
				AND ( NOT EXISTS (
                                SELECT
                                   ' '
                               FROM
                                    bec_ods.rcv_transactions_interface rti
                                WHERE is_deleted_flg <> 'Y'
                                    and rti.shipment_header_id = STG.shipment_header_id
                                    AND rti.shipment_line_id = STG.shipment_line_id
                                 )
		            )
				
    )SHP
);

END;

UPDATE
	BEC_ETL_CTRL.BATCH_DW_INFO
SET
	LOAD_TYPE = 'I',
	LAST_REFRESH_DATE = GETDATE()
WHERE
	DW_TABLE_NAME = 'fact_asn_shipment'
	AND BATCH_NAME = 'po';