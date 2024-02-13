 /*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.FACT_PO_CONSOLIDATED;

create table bec_dwh.FACT_PO_CONSOLIDATED diststyle all sortkey (PO_HEADER_ID,PO_LINE_ID)
as 	(select 
          POLL.ORG_ID,
          POH.PO_HEADER_ID,
          POL.PO_LINE_ID,
          POLL.LINE_LOCATION_ID,
          POD.PO_DISTRIBUTION_ID,
          POLL.PO_RELEASE_ID,
          POLL.SHIP_TO_ORGANIZATION_ID,
          POL.ITEM_ID  INVENTORY_ITEM_ID,
          POH.VENDOR_ID ,
          POH.VENDOR_SITE_ID,
          POH.AGENT_ID,
          POH.TERMS_ID,
          POD.CODE_COMBINATION_ID,
          POD.PROJECT_ID,
          POD.TASK_ID,
          REQ.REQUISITION_HEADER_ID,
          REQ.REQUISITION_LINE_ID, 
          REQ.DISTRIBUTION_ID REQ_DISTRIBUTION_ID,
          ODS.HEADER_ID HEADER_ID,
          ODS.LINE_ID LINE_ID,
          1 AS CATEGORY_SET_ID,
          POH.SEGMENT1                  PO_NUMBER,
          OOH.ORDER_NUMBER              DROP_SHIP_SALES_ORDER ,
          POH.TYPE_LOOKUP_CODE          PO_TYPE,
          DECODE(POH.TYPE_LOOKUP_CODE, 'STANDARD', POH.AUTHORIZATION_STATUS, POR.AUTHORIZATION_STATUS) AUTHORIZATION_STATUS,
          DECODE(POH.TYPE_LOOKUP_CODE, 'STANDARD', POH.CREATION_DATE, 'BLANKET', POR.RELEASE_DATE,            POH.CREATION_DATE) CREATION_DATE,
          POA1.AGENT_NAME               IM_BUYER,
          POR.RELEASE_NUM,
          POR.CANCEL_FLAG               BLNK_REL_CANCEL_FLAG,
          PLT.LINE_TYPE                 PO_LINE_TYPE,
          POL.LINE_NUM,
          POD.DISTRIBUTION_NUM,
          MSI.SEGMENT1                  PURCHASE_ITEM,
          POL.ITEM_ID,
          POL.ITEM_DESCRIPTION          PURCHASE_ITEM_DESCRIPTION,
          MSI.DESCRIPTION               ITEM_DESCRIPTION,
          POL.UNIT_MEAS_LOOKUP_CODE     PO_UOM,
          POL.ITEM_REVISION,
          MSI.PRIMARY_UNIT_OF_MEASURE   STD_UOM,
          POL.UNIT_PRICE,
          CIC.ITEM_COST                 STD_ITEM_COST,
          CIC.MATERIAL_COST,
          CIC.MATERIAL_OVERHEAD_COST,
          CIC.RESOURCE_COST,
          CIC.OVERHEAD_COST,
          CIC.OUTSIDE_PROCESSING_COST,
          POLL.QUANTITY                 ORDERED_QUANTITY,
          POLL.QUANTITY_RECEIVED,
          POLL.QUANTITY_BILLED,
          (POLL.QUANTITY - NVL(DECODE(POLL.MATCH_OPTION, 'R', POLL.QUANTITY_RECEIVED, POLL.QUANTITY_BILLED), 0) ) PO_OPEN_QTY,
          (POLL.QUANTITY_RECEIVED - POLL.QUANTITY_ACCEPTED - POLL.QUANTITY_REJECTED ) IQC_QUANTITY,
          (POLL.QUANTITY  - NVL(POLL.QUANTITY_BILLED,0)) PO_OPEN_QTY_FINANCE,
          (POLL.QUANTITY  - NVL(POLL.QUANTITY_RECEIVED,0)) PO_OPEN_QTY_RECEIVING,
          POLL.AMOUNT_RECEIVED,
          POLL.AMOUNT_BILLED,
          (POLL.QUANTITY * POL.UNIT_PRICE ) AMOUNT_ORDERED,
          (POLL.QUANTITY * POL.UNIT_PRICE - NVL(DECODE(POLL.MATCH_OPTION, 'R', DECODE(POLL.AMOUNT_RECEIVED, NULL, POLL.AMOUNT_BILLED, 0, 
	           POLL.AMOUNT_BILLED,POLL.AMOUNT_RECEIVED), POLL.AMOUNT_BILLED), 0) ) PO_OPEN_AMOUNT,
          POLL.NEED_BY_DATE,
          POLL.PROMISED_DATE,
          POH.COMMENTS, 
          NVL(POH.CLOSED_CODE, 'OPEN') PO_STATUS,
          NVL(POL.CLOSED_CODE, 'OPEN') PO_LINE_STATUS,
          NVL(POLL.CLOSED_CODE, 'OPEN') SHIPMENT_STATUS,
          DECODE(POLL.MATCH_OPTION, 'P', 'Purchase Order', 'R', 'Receipt', NULL) MATCH_OPTION,
          POLL.MATCHING_BASIS,
          DECODE(NVL(POLL.INSPECTION_REQUIRED_FLAG, 'N')
                 || POLL.RECEIPT_REQUIRED_FLAG, 'YY', '4-Way', 'NY', '3-Way',
                 'NN', '2-Way') MATCH_APPROVAL_LEVEL,
          (POLL.PROMISED_DATE + DECODE(POH.TERMS_ID, 10002, 10, 10003, 15,
                                        10004, 20, 10005, 25, 10006,
                                        28, 10007, 30, 10008, 45,
                                        10009, 60, 10010, 7, 10120,
                                        120, 10100, 90, 0) ) PAYMENT_DUE_DATE,
      --    POLL.LINE_LOCATION_ID,
          POH.FOB_LOOKUP_CODE FOB,
          POH.FREIGHT_TERMS_LOOKUP_CODE FREIGHT_TERMS,
          POH.SHIP_VIA_LOOKUP_CODE CARRIER,
          RRH.ROUTING_NAME RCV_ROUTING_NAME,
          POD.AMOUNT_BILLED     POD_AMOUNT_BILLED,
          POD.AMOUNT_DELIVERED  POD_AMOUNT_DELIVERED,
          POD.AMOUNT_ORDERED  POD_AMOUNT_ORDERED,
          POD.QUANTITY_BILLED POD_QUANTITY_BILLED,
          POD.QUANTITY_DELIVERED POD_QUANTITY_DELIVERED,
          POD.QUANTITY_ORDERED POD_QUANTITY_ORDERED,
          POLL.SHIPMENT_NUM,
          POLL.DROP_SHIP_FLAG,
          OOD.ORGANIZATION_NAME,
          REQ.REQUISITION_NUMBER ,
          REQ.REQUISITION_LINE,  
          REQ.EXPENDITURE_TYPE,
          --intransit.intransit_time,
          --po.promised_date - NVL (intransit.intransit_time, 0) supplier_ship_date,
          HL.LOCATION_CODE              SHIP_TO_LOCATION,
          nvl(HL.ADDRESS_LINE_1,'NA')     || ',' || nvl(HL.ADDRESS_LINE_2,'NA') SHIP_TO_ADDRESS,
          NVL(POLL.VMI_FLAG, 'N') VMI_FLAG,
          NVL(POLL.CONSIGNED_FLAG, 'N') CVMI_FLAG,
		  aprvr.employee_id as LAST_APPROVER,
		  --newly added columns
		  msi.attribute5 as program_name,
		  --audit columns
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
			)
			   || '-' || nvl(ODS.HEADER_ID, '0')
			   || '-' || nvl(ODS.LINE_ID, 0) 
			   || '-' || nvl(POLL.LINE_LOCATION_ID, '0')	
			   || '-' || nvl(POD.PO_DISTRIBUTION_ID, 0) 
			   as dw_load_id, 
			getdate() as dw_insert_date,
			getdate() as dw_update_date	
from
	(select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg<>'Y') POLL
	--172335
left outer join (select * from bec_ods.HR_LOCATIONS_ALL where is_deleted_flg<>'Y') hl on
	POLL.SHIP_TO_LOCATION_ID = HL.LOCATION_ID
left outer join (select * from bec_ods.PO_LINES_ALL where is_deleted_flg<>'Y') POL on
	POLL.PO_HEADER_ID = POL.PO_HEADER_ID
	and POLL.PO_LINE_ID = POL.PO_LINE_ID
left outer join (select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg<>'Y') poh on
	POL.PO_HEADER_ID = POH.PO_HEADER_ID
left outer join (select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg<>'Y') pod on
	POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID
left outer join (select * from bec_ods.PO_RELEASES_ALL where is_deleted_flg<>'Y') POR on
	POLL.PO_RELEASE_ID = POR.PO_RELEASE_ID
	and POLL.PO_HEADER_ID = POR.PO_HEADER_ID
left outer join (select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg<>'Y') MSI on
	POL.ITEM_ID = MSI.INVENTORY_ITEM_ID
	and POLL.SHIP_TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID
left outer join (select * from bec_ods.PO_LINE_TYPES_TL where is_deleted_flg<>'Y') PLT on
	POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID
	and PLT."language" = 'US'
left outer join (select * from bec_ods.PO_AGENTS_V where is_deleted_flg<>'Y') POA1 on
	MSI.BUYER_ID = POA1.AGENT_ID
left outer join (select * from bec_ods.CST_ITEM_COSTS where is_deleted_flg<>'Y') CIC on
	POL.ITEM_ID = CIC.INVENTORY_ITEM_ID
	and POLL.SHIP_TO_ORGANIZATION_ID = CIC.ORGANIZATION_ID
	and CIC.COST_TYPE_ID = 1
left outer join (select * from bec_ods.RCV_ROUTING_HEADERS where is_deleted_flg<>'Y') RRH on
	POLL.RECEIVING_ROUTING_ID = RRH.ROUTING_HEADER_ID
left outer join 
(select * from bec_ods.OE_DROP_SHIP_SOURCES where is_deleted_flg<>'Y') ODS 
on
	POLL.PO_HEADER_ID = ODS.PO_HEADER_ID
	and POLL.PO_LINE_ID = ODS.PO_LINE_ID
left outer join (select * from bec_ods.OE_ORDER_HEADERS_ALL where is_deleted_flg<>'Y') OOH on
	ODS.HEADER_ID = OOH.HEADER_ID
left outer join (select * from bec_ods.ORG_ORGANIZATION_DEFINITIONS where is_deleted_flg<>'Y') OOD on
	POLL.SHIP_TO_ORGANIZATION_ID = OOD.ORGANIZATION_ID
left outer join 
(
	select
		PRH.REQUISITION_HEADER_ID,
		PRL.REQUISITION_LINE_ID,
		PRD.DISTRIBUTION_ID,
		PRH.SEGMENT1 REQUISITION_NUMBER ,
		PRL.LINE_NUM REQUISITION_LINE,
		PRD.EXPENDITURE_TYPE
	from
		(select * from bec_ods.PO_REQ_DISTRIBUTIONS_ALL where is_deleted_flg<>'Y') PRD,
		(select * from bec_ods.PO_REQUISITION_LINES_ALL where is_deleted_flg<>'Y') PRL,
		(select * from bec_ods.PO_REQUISITION_HEADERS_ALL where is_deleted_flg<>'Y') PRH
	where
		1 = 1
		and PRD.REQUISITION_LINE_ID = PRL.REQUISITION_LINE_ID
		and PRL.REQUISITION_HEADER_ID = PRH.REQUISITION_HEADER_ID) REQ on
	POD.REQ_DISTRIBUTION_ID = REQ.DISTRIBUTION_ID
left outer join 
(	
select employee_id , object_id  from (select * from bec_ods.PO_ACTION_HISTORY where is_deleted_flg<>'Y')PO_ACTION_HISTORY
where 1=1 AND object_type_code = 'PO' and (object_id,sequence_num) in 
(select object_id,sequence_num from 
(SELECT MAX(SEQUENCE_NUM) as sequence_num ,object_id FROM (select * from bec_ods.PO_ACTION_HISTORY where is_deleted_flg<>'Y')PO_ACTION_HISTORY
WHERE 1=1
AND object_type_code = 'PO'
group by object_id
)
)
)aprvr
on aprvr.object_id = REQ.REQUISITION_HEADER_ID
where 1=1
	and ( POL.CANCEL_FLAG = 'N'
		or POL.CANCEL_FLAG is null )
		and ( POH.CANCEL_FLAG = 'N'
		or POH.CANCEL_FLAG is null )
	and ( POLL.CANCEL_FLAG = 'N'
		or POLL.CANCEL_FLAG is null )

);

END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_consolidated'
	and batch_name = 'po';