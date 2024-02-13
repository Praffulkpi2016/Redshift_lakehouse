/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
BEGIN;
--Delete Records
delete from 
  bec_dwh.FACT_PO_SHIPMENT_DETAILS 
where 
  (
    nvl(LINE_LOCATION_ID, 0)
  ) in (
    select 
      nvl(ods.LINE_LOCATION_ID, 0) as LINE_LOCATION_ID 
    from 
      bec_dwh.FACT_PO_SHIPMENT_DETAILS dw, 
      (
        select 
          distinct POLL.LINE_LOCATION_ID 
        from
		bec_ods.PO_LINE_LOCATIONS_ALL POLL
		left outer join bec_ods.HR_LOCATIONS_ALL hl 
		on
			POLL.SHIP_TO_LOCATION_ID = HL.LOCATION_ID
		left outer join bec_ods.PO_LINES_ALL POL 
		on
			POLL.PO_HEADER_ID = POL.PO_HEADER_ID
			and POLL.PO_LINE_ID = POL.PO_LINE_ID
		left outer join bec_ods.PO_HEADERS_ALL poh 
		on
			POL.PO_HEADER_ID = POH.PO_HEADER_ID
		left outer join bec_ods.PO_DISTRIBUTIONS_ALL pod 
		on
			POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID
		left outer join bec_ods.PO_RELEASES_ALL POR 
		on
			POLL.PO_RELEASE_ID = POR.PO_RELEASE_ID
			and POLL.PO_HEADER_ID = POR.PO_HEADER_ID
		left outer join bec_ods.MTL_SYSTEM_ITEMS_B MSI 
		on
			POL.ITEM_ID = MSI.INVENTORY_ITEM_ID
			and POLL.SHIP_TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID
		left outer join bec_ods.PO_LINE_TYPES_TL PLT 
		on
			POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID
			and PLT."language" = 'US'
		left outer join bec_ods.PO_AGENTS_V POA1 
		on
			MSI.BUYER_ID = POA1.AGENT_ID
		left outer join bec_ods.CST_ITEM_COSTS CIC 
		on
			POL.ITEM_ID = CIC.INVENTORY_ITEM_ID
			and POLL.SHIP_TO_ORGANIZATION_ID = CIC.ORGANIZATION_ID
			and CIC.COST_TYPE_ID = 1
		left outer join bec_ods.RCV_ROUTING_HEADERS RRH 
		on
			POLL.RECEIVING_ROUTING_ID = RRH.ROUTING_HEADER_ID
		left outer join bec_ods.ORG_ORGANIZATION_DEFINITIONS OOD 
		on
			POLL.SHIP_TO_ORGANIZATION_ID = OOD.ORGANIZATION_ID 
        where 
          1 = 1 
          and (
            POL.CANCEL_FLAG = 'N' 
            or POL.CANCEL_FLAG is null
          ) 
          and (
            POH.CANCEL_FLAG = 'N' 
            or POH.CANCEL_FLAG is null
          ) 
          and (
            POLL.CANCEL_FLAG = 'N' 
            or POLL.CANCEL_FLAG is null
          ) 
          and (
		  POLL.kca_seq_date > (
            select 
              (executebegints - prune_days) 
            from 
              bec_etl_ctrl.batch_dw_info 
            where 
              dw_table_name = 'fact_po_shipment_details' 
              and batch_name = 'po')
		  OR POLL.is_deleted_flg = 'Y'
		  )
      ) ods 
    where 
      dw.dw_load_id = (
        select 
          system_id 
        from 
          bec_etl_ctrl.etlsourceappid 
        where 
          source_system = 'EBS'
      ) || '-' || nvl(ods.LINE_LOCATION_ID, 0)
  );
commit;
-- Insert records
INSERT INTO bec_dwh.FACT_PO_SHIPMENT_DETAILS (
  ORG_ID,
  PO_HEADER_ID,
  PO_LINE_ID,
  LINE_LOCATION_ID,
  PO_RELEASE_ID,
  SHIP_TO_ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  VENDOR_ID ,
  VENDOR_SITE_ID,
  AGENT_ID,
  TERMS_ID,
  CATEGORY_SET_ID,
  PO_NUMBER,
  PO_TYPE,
  AUTHORIZATION_STATUS,
  CREATION_DATE,
  IM_BUYER,
  RELEASE_NUM,
  BLNK_REL_CANCEL_FLAG,
  PO_LINE_TYPE,
  LINE_NUM,
  PURCHASE_ITEM,
  ITEM_ID,
  PURCHASE_ITEM_DESCRIPTION,
  ITEM_DESCRIPTION,
  PO_UOM,
  ITEM_REVISION,
  STD_UOM,
  UNIT_PRICE,
  STD_ITEM_COST,
  MATERIAL_COST,
  MATERIAL_OVERHEAD_COST,
  RESOURCE_COST,
  OVERHEAD_COST,
  OUTSIDE_PROCESSING_COST,
  ORDERED_QUANTITY,
  QUANTITY_RECEIVED,
  QUANTITY_BILLED,
  PO_OPEN_QTY,
  IQC_QUANTITY,
  PO_OPEN_QTY_FINANCE,
  PO_OPEN_QTY_RECEIVING,
  AMOUNT_RECEIVED,
  AMOUNT_BILLED,
  AMOUNT_ORDERED,
  PO_OPEN_AMOUNT,
  NEED_BY_DATE,
  PROMISED_DATE,
  SUPPLY_DELAY,
  PROMISE_MONTH,
  COMMENTS, 
  PO_STATUS,
  PO_LINE_STATUS,
  SHIPMENT_STATUS,
  MATCH_OPTION,
  MATCHING_BASIS,
  MATCH_APPROVAL_LEVEL,
  PAYMENT_DUE_DATE,
  PAYMENT_MONTH,
  FOB,
  FREIGHT_TERMS,
  CARRIER,
  RCV_ROUTING_NAME,
  SHIPMENT_NUM,
  DROP_SHIP_FLAG,
  ORGANIZATION_NAME,
  SHIP_TO_LOCATION,
  SHIP_TO_ADDRESS,
  VMI_FLAG,
  CVMI_FLAG,
  program_name,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
) (
  select 
    distinct POLL.ORG_ID, 
    POH.PO_HEADER_ID, 
    POL.PO_LINE_ID, 
    POLL.LINE_LOCATION_ID, 
    POLL.PO_RELEASE_ID, 
    POLL.SHIP_TO_ORGANIZATION_ID, 
    POL.ITEM_ID INVENTORY_ITEM_ID, 
    POH.VENDOR_ID, 
    POH.VENDOR_SITE_ID, 
    POH.AGENT_ID, 
    POH.TERMS_ID, 
    1 AS CATEGORY_SET_ID, 
    POH.SEGMENT1 PO_NUMBER, 
    POH.TYPE_LOOKUP_CODE PO_TYPE, 
    DECODE(
      POH.TYPE_LOOKUP_CODE, 'STANDARD', 
      POH.AUTHORIZATION_STATUS, POR.AUTHORIZATION_STATUS
    ) AUTHORIZATION_STATUS, 
    DECODE(
      POH.TYPE_LOOKUP_CODE, 'STANDARD', 
      POH.CREATION_DATE, 'BLANKET', POR.RELEASE_DATE, 
      POH.CREATION_DATE
    ) CREATION_DATE, 
    POA1.AGENT_NAME IM_BUYER, 
    POR.RELEASE_NUM, 
    POR.CANCEL_FLAG BLNK_REL_CANCEL_FLAG, 
    PLT.LINE_TYPE PO_LINE_TYPE, 
    POL.LINE_NUM, 
    MSI.SEGMENT1 PURCHASE_ITEM, 
    POL.ITEM_ID, 
    POL.ITEM_DESCRIPTION PURCHASE_ITEM_DESCRIPTION, 
    MSI.DESCRIPTION ITEM_DESCRIPTION, 
    POL.UNIT_MEAS_LOOKUP_CODE PO_UOM, 
    POL.ITEM_REVISION, 
    MSI.PRIMARY_UNIT_OF_MEASURE STD_UOM, 
    POL.UNIT_PRICE, 
    CIC.ITEM_COST STD_ITEM_COST, 
    CIC.MATERIAL_COST, 
    CIC.MATERIAL_OVERHEAD_COST, 
    CIC.RESOURCE_COST, 
    CIC.OVERHEAD_COST, 
    CIC.OUTSIDE_PROCESSING_COST, 
    POLL.QUANTITY ORDERED_QUANTITY, 
    POLL.QUANTITY_RECEIVED, 
    POLL.QUANTITY_BILLED, 
    (
      POLL.QUANTITY - NVL(
        DECODE(
          POLL.MATCH_OPTION, 'R', POLL.QUANTITY_RECEIVED, 
          POLL.QUANTITY_BILLED
        ), 
        0
      )
    ) PO_OPEN_QTY, 
    (
      POLL.QUANTITY_RECEIVED - POLL.QUANTITY_ACCEPTED - POLL.QUANTITY_REJECTED
    ) IQC_QUANTITY, 
    (
      POLL.QUANTITY - NVL(POLL.QUANTITY_BILLED, 0)
    ) PO_OPEN_QTY_FINANCE, 
    (
      POLL.QUANTITY - NVL(POLL.QUANTITY_RECEIVED, 0)
    ) PO_OPEN_QTY_RECEIVING, 
    POLL.AMOUNT_RECEIVED, 
    POLL.AMOUNT_BILLED, 
    (POLL.QUANTITY * POL.UNIT_PRICE) AMOUNT_ORDERED, 
    (
      POLL.QUANTITY * POL.UNIT_PRICE - NVL(
        DECODE(
          POLL.MATCH_OPTION, 
          'R', 
          DECODE(
            POLL.AMOUNT_RECEIVED, NULL, POLL.AMOUNT_BILLED, 
            0, POLL.AMOUNT_BILLED, POLL.AMOUNT_RECEIVED
          ), 
          POLL.AMOUNT_BILLED
        ), 
        0
      )
    ) PO_OPEN_AMOUNT, 
    POLL.NEED_BY_DATE, 
    POLL.PROMISED_DATE, 
    datediff(hours, PROMISED_DATE, SYSDATE) SUPPLY_DELAY, 
    to_char(POLL.PROMISED_DATE, 'MON-YY') PROMISE_MONTH, 
    POH.COMMENTS, 
    NVL(POH.CLOSED_CODE, 'OPEN') PO_STATUS, 
    NVL(POL.CLOSED_CODE, 'OPEN') PO_LINE_STATUS, 
    NVL(POLL.CLOSED_CODE, 'OPEN') SHIPMENT_STATUS, 
    DECODE(
      POLL.MATCH_OPTION, 'P', 'Purchase Order', 
      'R', 'Receipt', NULL
    ) MATCH_OPTION, 
    POLL.MATCHING_BASIS, 
    DECODE(
      NVL(
        POLL.INSPECTION_REQUIRED_FLAG, 'N'
      ) || POLL.RECEIPT_REQUIRED_FLAG, 
      'YY', 
      '4-Way', 
      'NY', 
      '3-Way', 
      'NN', 
      '2-Way'
    ) MATCH_APPROVAL_LEVEL, 
    (
      POLL.PROMISED_DATE + DECODE(
        POH.TERMS_ID, 10002, 10, 10003, 15, 
        10004, 20, 10005, 25, 10006, 28, 10007, 
        30, 10008, 45, 10009, 60, 10010, 7, 10120, 
        120, 10100, 90, 0
      )
    ) PAYMENT_DUE_DATE, 
    to_char(
      POLL.PROMISED_DATE + DECODE(
        POH.TERMS_ID, 10002, 10, 10003, 15, 
        10004, 20, 10005, 25, 10006, 28, 10007, 
        30, 10008, 45, 10009, 60, 10010, 7, 10120, 
        120, 10100, 90, 0
      ), 
      'MON-YY'
    ) PAYMENT_MONTH, 
    POH.FOB_LOOKUP_CODE FOB, 
    POH.FREIGHT_TERMS_LOOKUP_CODE FREIGHT_TERMS, 
    POH.SHIP_VIA_LOOKUP_CODE CARRIER, 
    RRH.ROUTING_NAME RCV_ROUTING_NAME, 
    POLL.SHIPMENT_NUM, 
    POLL.DROP_SHIP_FLAG, 
    OOD.ORGANIZATION_NAME, 
    HL.LOCATION_CODE SHIP_TO_LOCATION, 
    nvl(HL.ADDRESS_LINE_1, 'NA') || ',' || nvl(HL.ADDRESS_LINE_2, 'NA') SHIP_TO_ADDRESS, 
    NVL(POLL.VMI_FLAG, 'N') VMI_FLAG, 
    NVL(POLL.CONSIGNED_FLAG, 'N') CVMI_FLAG, 
	msi.attribute5 as program_name,
    'N' as is_deleted_flg, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    ) as source_app_id, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    ) || '-' || nvl(POLL.LINE_LOCATION_ID, '0') as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  from
	(select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg<>'Y') POLL
	left outer join (select * from bec_ods.HR_LOCATIONS_ALL where is_deleted_flg<>'Y') hl 
	on
		POLL.SHIP_TO_LOCATION_ID = HL.LOCATION_ID
	left outer join (select * from bec_ods.PO_LINES_ALL where is_deleted_flg<>'Y') POL 
	on
		POLL.PO_HEADER_ID = POL.PO_HEADER_ID
		and POLL.PO_LINE_ID = POL.PO_LINE_ID
	left outer join (select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg<>'Y') poh 
	on
		POL.PO_HEADER_ID = POH.PO_HEADER_ID
	left outer join (select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg<>'Y') pod 
	on
		POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID
	left outer join (select * from bec_ods.PO_RELEASES_ALL where is_deleted_flg<>'Y') POR 
	on
		POLL.PO_RELEASE_ID = POR.PO_RELEASE_ID
		and POLL.PO_HEADER_ID = POR.PO_HEADER_ID
	left outer join (select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg<>'Y') MSI 
	on
		POL.ITEM_ID = MSI.INVENTORY_ITEM_ID
		and POLL.SHIP_TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID
	left outer join (select * from bec_ods.PO_LINE_TYPES_TL where is_deleted_flg<>'Y') PLT 
	on
		POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID
		and PLT."language" = 'US'
	left outer join (select * from bec_ods.PO_AGENTS_V where is_deleted_flg<>'Y') POA1 
	on
		MSI.BUYER_ID = POA1.AGENT_ID
	left outer join (select * from bec_ods.CST_ITEM_COSTS where is_deleted_flg<>'Y') CIC 
	on
		POL.ITEM_ID = CIC.INVENTORY_ITEM_ID
		and POLL.SHIP_TO_ORGANIZATION_ID = CIC.ORGANIZATION_ID
		and CIC.COST_TYPE_ID = 1
	left outer join (select * from bec_ods.RCV_ROUTING_HEADERS where is_deleted_flg<>'Y') RRH 
	on
		POLL.RECEIVING_ROUTING_ID = RRH.ROUTING_HEADER_ID
	left outer join (select * from bec_ods.ORG_ORGANIZATION_DEFINITIONS where is_deleted_flg<>'Y') OOD 
	on
		POLL.SHIP_TO_ORGANIZATION_ID = OOD.ORGANIZATION_ID 
  where 
    1 = 1 
    and (
      POL.CANCEL_FLAG = 'N' 
      or POL.CANCEL_FLAG is null
    ) 
    and (
      POH.CANCEL_FLAG = 'N' 
      or POH.CANCEL_FLAG is null
    ) 
    and (
      POLL.CANCEL_FLAG = 'N' 
      or POLL.CANCEL_FLAG is null
    ) 
    and POLL.kca_seq_date > (
      select 
        (executebegints - prune_days) 
      from 
        bec_etl_ctrl.batch_dw_info 
      where 
        dw_table_name = 'fact_po_shipment_details' 
        and batch_name = 'po'
    )
);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_po_shipment_details' 
  and batch_name = 'po';
commit;
