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

drop table if exists bec_dwh.FACT_OM_SHIPMENT;

create table bec_dwh.FACT_OM_SHIPMENT
	diststyle all sortkey(DELIVERY_DETAIL_ID)
as
(
SELECT	 ORDER_HEADER_ID
        ,DELIVERY_DETAIL_ID
        ,MOVE_ORDER_LINE_ID
        ,ORDER_LINE_ID   
		,ORG_ID
		,ORGANIZATION_ID
		,INVENTORY_ITEM_ID
        ,DELIVERY_NUMBER
		,DELIVERY_NAME
        ,MOVE_ORDER_NUMBER
		,MO_TRANSACTION_ID 
		,DATE_SCHEDULED
		,RELEASED_STATUS   --Lookup 'PICK_STATUS'
		,TRACKING_NUMBER
		,WAYBILL
		,SHIP_METHOD_CODE   --join with lookups dimension SHIP_METHOD
		,BILL_OF_LADDING
		,FREIGHT_CARRIER
		,SERIAL_NUMBER
		,FROM_SUB_INVENTORY
		,DELIVER_TO_SITE_USE_ID
		,DELIVER_TO_LOCATION_ID
		,DELIVER_TO_CONTACT_ID  --Need to check
		,DELIVER_TO
		,SHIPMENT_NUMBER
		,ACCEPTED_DATE 
		,ACTION_REQUIRED
		,REQUESTED_QUANTITY
		,PICKED_QUANTITY
		,DATE_REQUESTED
		,DATE_SCHEDULED SHIPPING_DATE
		,CREATION_DATE
		,PICK_SLIP_NUMBER
		,PICK_RELEASE_DATE
		,ORDER_TYPE_ID
		,LINE_TYPE_ID
		,DELIVERED_DATE
		,SALESREP_ID
		,ORDER_SOURCE_ID
		,SOURCE_TYPE_CODE
		,PICKABLE_FLAG     --Added to fix UAT issue.
		,DELIVERY_STATUS   --Added to fix UAT issue.
		,ACCOUNT_EXECUTIVE --Added to fix UAT issue.
		,'N' as is_deleted_flg,
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
||'-'|| nvl(DELIVERY_DETAIL_ID, 0) as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM 
( SELECT WDD.SOURCE_HEADER_ID ORDER_HEADER_ID
        ,WDD.DELIVERY_DETAIL_ID
        ,WDD.MOVE_ORDER_LINE_ID
        ,WDD.SOURCE_LINE_ID   ORDER_LINE_ID
		,WDD.ORG_ID
		,WDD.ORGANIZATION_ID
		,WDD.INVENTORY_ITEM_ID
        ,WND.DELIVERY_ID  DELIVERY_NUMBER
		,WND.NAME  DELIVERY_NAME
        ,MTRH.REQUEST_NUMBER MOVE_ORDER_NUMBER
		,MAX(NVL(mmtt.transaction_temp_id, MMT.TRANSACTION_ID))  MO_TRANSACTION_ID 
		,WDD.DATE_SCHEDULED
		,WDD.RELEASED_STATUS   --Lookup 'PICK_STATUS'
		,WDD.TRACKING_NUMBER
		,WND.WAYBILL
		,WnD.SHIP_METHOD_CODE   --join with lookups dimension SHIP_METHOD
		,WDI.SEQUENCE_NUMBER BILL_OF_LADDING
		,WC.FREIGHT_CODE FREIGHT_CARRIER
		,WDD.SERIAL_NUMBER  SERIAL_NUMBER
		,WDD.SUBINVENTORY FROM_SUB_INVENTORY
		,WDD.DELIVER_TO_SITE_USE_ID
		,WDD.DELIVER_TO_LOCATION_ID
		,WDD.DELIVER_TO_CONTACT_ID  --Need to check
		,NVL(HL.ADDRESS1,'NA')||'-'||NVL(HL.ADDRESS2,'NA')||'-'||NVL(HL.CITY,'NA')||'-'||NVL(HL.STATE,'NA')||'-'||NVL(HL.POSTAL_CODE,'NA')
		||'-'||NVL(HL.COUNTRY,'NA') DELIVER_TO
		,WDD.SHIPMENT_LINE_NUMBER  SHIPMENT_NUMBER
		--,MTRH.REQUEST_NUMBER MOVE_ORDER_NUMBER
		--,MMT.TRANSACTION_ID MO_TRANSACTION_ID
		,WND.ACCEPTED_DATE 
		,DECODE(WDD.RELEASED_STATUS,'Y', 'Ship Confirm','S','Pick-Confirm/Transact Move Order',
		                            'C','Not Applicable','R','Pick Release','None') ACTION_REQUIRED
		,WDD.REQUESTED_QUANTITY
		,WDD.PICKED_QUANTITY
		,WDD.DATE_REQUESTED
		,WDD.DATE_SCHEDULED SHIPPING_DATE
		,WDD.CREATION_DATE
		,MAX(COALESCE(MMTT.pick_slip_number,mmt.PICK_SLIP_NUMBER,MTRL.PICK_SLIP_NUMBER)) PICK_SLIP_NUMBER
		,MAX(COALESCE(MMTT.creation_date, mmt.creation_date, MTRL.PICK_SLIP_DATE))  PICK_RELEASE_DATE
		,OOH.ORDER_TYPE_ID
		,OOL.LINE_TYPE_ID
		,WND.DELIVERED_DATE
		,OOH.SALESREP_ID
		,OOH.ORDER_SOURCE_ID
		,OOL.SOURCE_TYPE_CODE
		,WDD.PICKABLE_FLAG  --Added to fix UAT issue.
		,DECODE(WND.STATUS_CODE,'OP','Open', 'CL', 'Closed') DELIVERY_STATUS --Added to fix UAT issue.
		--,DECODE(UPPER(OOH.ATTRIBUTE8),'INDIA', '999999', OOH.ATTRIBUTE8) ACCOUNT_EXECUTIVE--Added to fix UAT issue.	
		,DECODE(case when OOH.attribute8 = ' India' then 'INDIA' else UPPER(OOH.ATTRIBUTE8) end,'INDIA', '999999', OOH.ATTRIBUTE8)   ACCOUNT_EXECUTIVE -- Added to fix PROD issue
	FROM	
		  (select * from bec_ods.WSH_DELIVERY_DETAILS where is_deleted_flg <> 'Y') WDD
		 ,(select * from bec_ods.WSH_DELIVERY_ASSIGNMENTS where is_deleted_flg <> 'Y') WDA
         ,(select * from bec_ods.WSH_NEW_DELIVERIES  where is_deleted_flg <> 'Y')WND
         ,(select * from bec_ods.WSH_CARRIERS where is_deleted_flg <> 'Y') WC
		 --,WSH_EXCEPTIONS             WEXP
		 ,(select * from bec_ods.WSH_DELIVERY_LEGS where is_deleted_flg <> 'Y')WDL
		 ,(select * from bec_ods.WSH_DOCUMENT_INSTANCES where is_deleted_flg <> 'Y')WDI
		 ,(select * from bec_ods.MTL_TXN_REQUEST_LINES where is_deleted_flg <> 'Y')MTRL		 
	     ,(select * from bec_ods.MTL_TXN_REQUEST_HEADERS where is_deleted_flg <> 'Y')MTRH
	     ,(select * from bec_ods.mtl_material_transactions_temp where is_deleted_flg <> 'Y')mmtt
	     ,(select * from bec_ods.MTL_MATERIAL_TRANSACTIONS where is_deleted_flg <> 'Y')MMT
		 ,(select * from bec_ods.OE_ORDER_HEADERS_ALL where is_deleted_flg <> 'Y')OOH
		 ,(select * from bec_ods.OE_ORDER_LINES_ALL where is_deleted_flg <> 'Y')OOL
		 ,(select * from bec_ods.HZ_LOCATIONS where is_deleted_flg <> 'Y')HL
WHERE 		1 = 1
		AND WDD.RELEASED_STATUS <> 'D'
		AND WDD.DELIVERY_DETAIL_ID = WDA.DELIVERY_DETAIL_ID(+)
        AND WDA.DELIVERY_ID = WND.DELIVERY_ID (+)
        AND WND.CARRIER_ID  = WC.CARRIER_ID (+)
		--AND WND.DELIVERY_ID = WEXP.DELIVERY_ID(+)
		AND WND.DELIVERY_ID = WDL.DELIVERY_ID(+)
		AND WDL.delivery_leg_id = WDI.ENTITY_ID (+)
		AND WDI.ENTITY_NAME (+) = 'WSH_DELIVERY_LEGS'
		AND WDI.DOCUMENT_TYPE (+) = 'BOL'
		--AND WDD.DELIVERY_DETAIL_ID = MMT.PICKING_LINE_ID(+)
		AND WDD.MOVE_ORDER_LINE_ID = MTRL.LINE_ID (+) 
		AND MTRL.HEADER_ID = MTRH.HEADER_ID(+)
		AND MTRL.LINE_ID   = MMT.MOVE_ORDER_LINE_ID(+)
		AND MTRL.LINE_ID   = MMTT.MOVE_ORDER_LINE_ID(+)
		AND (MMT.TRANSACTION_TYPE_ID is null or MMT.TRANSACTION_TYPE_ID = 52 or MMT.TRANSACTION_TYPE_ID = 53)
		--AND MMT.TRANSFER_SUBINVENTORY(+)  <> 'STAGE' 
		--AND MTRL.FROM_SUBINVENTORY_CODE = MMT.SUBINVENTORY_CODE(+)
		--AND MTRL.TO_SUBINVENTORY_CODE  = MMT.TRANSFER_SUBINVENTORY(+)
		--AND MMT.TRANSACTION_QUANTITY(+) > 0
		AND WDD.SOURCE_HEADER_ID = OOL.HEADER_ID
		AND WDD.SOURCE_LINE_ID   = OOL.LINE_ID
		AND OOL.HEADER_ID        = OOH.HEADER_ID
		AND OOL.ORG_ID           = OOH.ORG_ID
		AND WDD.DELIVER_TO_LOCATION_ID  = HL.LOCATION_ID(+)	
  GROUP BY WDD.SOURCE_HEADER_ID 
        ,WDD.DELIVERY_DETAIL_ID
        ,WDD.MOVE_ORDER_LINE_ID
        ,WDD.SOURCE_LINE_ID   
		,WDD.ORG_ID
		,WDD.ORGANIZATION_ID
		,WDD.INVENTORY_ITEM_ID
        ,WND.DELIVERY_ID  
		,WND.NAME  
        ,MTRH.REQUEST_NUMBER 
		--,MAX(NVL(mmtt.transaction_temp_id, MMT.TRANSACTION_ID))   
		,WDD.DATE_SCHEDULED
		,WDD.RELEASED_STATUS   --Lookup 'PICK_STATUS'
		,WDD.TRACKING_NUMBER
		,WND.WAYBILL
		,WnD.SHIP_METHOD_CODE   --join with lookups dimension SHIP_METHOD
		,WDI.SEQUENCE_NUMBER 
		,WC.FREIGHT_CODE 
		,WDD.SERIAL_NUMBER  
		,WDD.SUBINVENTORY 
		,WDD.DELIVER_TO_SITE_USE_ID
		,WDD.DELIVER_TO_LOCATION_ID
		,WDD.DELIVER_TO_CONTACT_ID  --Need to check
		,NVL(HL.ADDRESS1,'NA')||'-'||NVL(HL.ADDRESS2,'NA')||'-'||NVL(HL.CITY,'NA')||'-'||NVL(HL.STATE,'NA')||'-'||NVL(HL.POSTAL_CODE,'NA')
		||'-'||NVL(HL.COUNTRY,'NA') 
		,WDD.SHIPMENT_LINE_NUMBER  
		--,MTRH.REQUEST_NUMBER MOVE_ORDER_NUMBER
		--,MMT.TRANSACTION_ID MO_TRANSACTION_ID
		,WND.ACCEPTED_DATE 
		,DECODE(WDD.RELEASED_STATUS,'Y', 'Ship Confirm','S','Pick-Confirm/Transact Move Order',
		                            'C','Not Applicable','R','Pick Release','None') 
		,WDD.REQUESTED_QUANTITY
		,WDD.PICKED_QUANTITY
		,WDD.DATE_REQUESTED
		,WDD.DATE_SCHEDULED 
		,WDD.CREATION_DATE
		--,MAX(COALESCE(MMTT.pick_slip_number,mmt.PICK_SLIP_NUMBER,MTRL.PICK_SLIP_NUMBER)) 
		--,MAX(COALESCE(MMTT.creation_date, mmt.creation_date, MTRL.PICK_SLIP_DATE))  PICK_RELEASE_DATE
		,OOH.ORDER_TYPE_ID
		,OOL.LINE_TYPE_ID
		,WND.DELIVERED_DATE
		,OOH.SALESREP_ID
		,OOH.ORDER_SOURCE_ID
		,OOL.SOURCE_TYPE_CODE
		,WDD.PICKABLE_FLAG  --Added to fix UAT issue.
		,DECODE(WND.STATUS_CODE,'OP','Open', 'CL', 'Closed')  --Added to fix UAT issue.
		,DECODE(case when OOH.attribute8 = ' India' then 'INDIA' else UPPER(OOH.ATTRIBUTE8) end,'INDIA', '999999', OOH.ATTRIBUTE8)
	)
);
commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_om_shipment'
	and batch_name = 'om';
commit;