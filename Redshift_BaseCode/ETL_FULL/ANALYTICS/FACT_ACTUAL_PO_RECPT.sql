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

drop table if exists bec_dwh.FACT_ACTUAL_PO_RECPT ;

create table bec_dwh.FACT_ACTUAL_PO_RECPT diststyle all 
sortkey(line_location_id ,organization_id)
as 
(
select
	a."WIP_ENTITY_NAME",
	a."PO_TYPE",
	a."PO_NUMBER",
	a."PR_APPROVED_DATE",
	a."REACTION_TIME",
	a."PO_PLACED_ONTIME",
	a."BUYER_NAME",
	a."PO_RELEASE_NUMBER",
	a."PO_SHIPMENT_NUM",
	a."PO_DATE",
	a.po_last_update_date,
	a.processing_leadtime,
	a."PO_LINE_NUM",
	a."ITEM_NAME",
	a."ITEM_DESCRIPTION",
	a."LINE_QUANTITY",
	a."QUANTITY_SHIPPED",
	a."PO_UNIT_OF_MEASURE",
	a."PRIMARY_UOM_CODE",
	a."PRIMARY_UNIT_OF_MEASURE",
	a."NEED_BY_DATE",
	a."PROMISED_DATE",
	a."RCV_QUANTITY_RECEIVED",
	a.primary_quantity,
	a."PO_UNIT_PRICE",
	a."EXTENDED_PO_RCV_PRICE",
	a."RECEIPT_NUMBER",
	a."PACKING_SLIP",
	a."SHIPMENT_NUM",
	a.waybill_airbill_num,
	a.bill_of_lading,
	a."SHIPMENT_DATE",
	a."VENDOR_NUMBER",
	a."VENDOR_NAME",
	a."CLOSED_CODE",
	a."PO_CURRENCY_CODE",
	a."SHIPMENT_HEADER_ID" ,	
     'N' AS IS_DELETED_FLG,
		  (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || a.SHIPMENT_HEADER_ID as SHIPMENT_HEADER_ID_KEY,
	a."SHIPMENT_LINE_ID" ,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || a.SHIPMENT_LINE_ID as SHIPMENT_LINE_ID_KEY,
	a."PO_LINE_TYPE",
	a."LINE_LOCATION_ID" ,            
		  (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || a.LINE_LOCATION_ID as LINE_LOCATION_ID_KEY,
	a."UNIT_OF_MEASURE",
	a."PO_CONVERSION",
	a."RCV_CONVERSION",
	a."ASN_TYPE",
	a."ASN_STATUS",
	a.RECEIPT_DATE::TIMESTAMP,
	a."STD_ITEM_COST",
	a."MATERIAL_COST",
	a.ext_material_cost,
	a.purchase_price_variance,
	a."OUTSIDE_PROCESSING_COST",
	a."OVERHEAD_COST",
	a."MATERIAL_OVERHEAD_COST",
	a."RESOURCE_COST",
	a."FOB_CODE",
	a."SHIP_TO_LOCATION",
	a."SHIP_TO_ORGANIZATION",
	a.organization_code,
	a.organization_id , 
	NVL(b.vmi_flag, 'N') vmi_flag,
		  (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || a.organization_id as organization_id_KEY,
			  (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || ood.operating_unit as org_id_KEY,
	ood.operating_unit as org_id,
	a."CATEGORY",
	a."ITEM_REVISION",
	a."ATTRIBUTE10",
	a."ACTUAL_COMMIT_NEED_DATE",
	a."FREIGHT_TERMS",
	a."CVMI_FLAG",
	a."VENDOR_COUNTRY",			 
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
    || '-' || nvl(a.po_number, '0') 
    || '-' || nvl(a.po_line_num, 0)
   || '-' || nvl(a.line_location_id, 0) 
   || '-' || nvl(a.shipment_header_id, 0)
   || '-' || nvl(a.shipment_line_id, 0)
   || '-' || nvl(a.wip_entity_name, '0')
   || '-' || NVL(TO_CHAR(receipt_date, 'YYYYMMDD'), 'NA')
	  as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.bec_actual_po_recpt1 where is_deleted_flg <> 'Y') a,
	(select * from bec_ods.po_line_locations_all where is_deleted_flg <> 'Y') b,
	(select * from bec_ods.org_organization_definitions where is_deleted_flg <> 'Y') ood 
where a.line_location_id = b.line_location_id (+)
and a.organization_id  = ood.organization_id
)
;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_actual_po_recpt'
	and batch_name = 'po';

commit;