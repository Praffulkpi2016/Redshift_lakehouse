/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.rcv_shipment_lines
where SHIPMENT_LINE_ID in (
select stg.SHIPMENT_LINE_ID from bec_ods.rcv_shipment_lines ods, bec_ods_stg.rcv_shipment_lines stg
where ods.SHIPMENT_LINE_ID = stg.SHIPMENT_LINE_ID and stg.kca_operation in ('INSERT','UPDATE'));

commit;


-- Insert records
insert
	into
	bec_ods.RCV_SHIPMENT_LINES
(SHIPMENT_LINE_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	SHIPMENT_HEADER_ID,
	LINE_NUM,
	CATEGORY_ID,
	QUANTITY_SHIPPED,
	QUANTITY_RECEIVED,
	UNIT_OF_MEASURE,
	ITEM_DESCRIPTION,
	ITEM_ID,
	ITEM_REVISION,
	VENDOR_ITEM_NUM,
	VENDOR_LOT_NUM,
	UOM_CONVERSION_RATE,
	SHIPMENT_LINE_STATUS_CODE,
	SOURCE_DOCUMENT_CODE,
	PO_HEADER_ID,
	PO_RELEASE_ID,
	PO_LINE_ID,
	PO_LINE_LOCATION_ID,
	PO_DISTRIBUTION_ID,
	REQUISITION_LINE_ID,
	REQ_DISTRIBUTION_ID,
	ROUTING_HEADER_ID,
	PACKING_SLIP,
	FROM_ORGANIZATION_ID,
	DELIVER_TO_PERSON_ID,
	EMPLOYEE_ID,
	DESTINATION_TYPE_CODE,
	TO_ORGANIZATION_ID,
	TO_SUBINVENTORY,
	LOCATOR_ID,
	DELIVER_TO_LOCATION_ID,
	CHARGE_ACCOUNT_ID,
	TRANSPORTATION_ACCOUNT_ID,
	SHIPMENT_UNIT_PRICE,
	TRANSFER_COST,
	TRANSPORTATION_COST,
	COMMENTS,
	ATTRIBUTE_CATEGORY,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
	REASON_ID,
	USSGL_TRANSACTION_CODE,
	GOVERNMENT_CONTEXT,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	DESTINATION_CONTEXT,
	PRIMARY_UNIT_OF_MEASURE,
	EXCESS_TRANSPORT_REASON,
	EXCESS_TRANSPORT_RESPONSIBLE,
	EXCESS_TRANSPORT_AUTH_NUM,
	ASN_LINE_FLAG,
	ORIGINAL_ASN_PARENT_LINE_ID,
	ORIGINAL_ASN_LINE_FLAG,
	VENDOR_CUM_SHIPPED_QUANTITY,
	NOTICE_UNIT_PRICE,
	TAX_NAME,
	TAX_AMOUNT,
	INVOICE_STATUS_CODE,
	CUM_COMPARISON_FLAG,
	CONTAINER_NUM,
	TRUCK_NUM,
	BAR_CODE_LABEL,
	TRANSFER_PERCENTAGE,
	MRC_SHIPMENT_UNIT_PRICE,
	MRC_TRANSFER_COST,
	MRC_TRANSPORTATION_COST,
	MRC_NOTICE_UNIT_PRICE,
	SHIP_TO_LOCATION_ID,
	COUNTRY_OF_ORIGIN_CODE,
	OE_ORDER_HEADER_ID,
	OE_ORDER_LINE_ID,
	CUSTOMER_ITEM_NUM,
	COST_GROUP_ID,
	SECONDARY_QUANTITY_SHIPPED,
	SECONDARY_QUANTITY_RECEIVED,
	SECONDARY_UNIT_OF_MEASURE,
	QC_GRADE,
	MMT_TRANSACTION_ID,
	ASN_LPN_ID,
	AMOUNT,
	AMOUNT_RECEIVED,
	JOB_ID,
	TIMECARD_ID,
	TIMECARD_OVN,
	OSA_FLAG,
	REQUESTED_AMOUNT,
	MATERIAL_STORED_AMOUNT,
	APPROVAL_STATUS,
	AMOUNT_SHIPPED,
	LCM_SHIPMENT_LINE_ID,
	UNIT_LANDED_COST,
	EQUIPMENT_ID,
KCA_OPERATION,
IS_DELETED_FLG
,kca_seq_id
,kca_seq_date
	)	
(
select
	shipment_line_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	shipment_header_id,
	line_num,
	category_id,
	quantity_shipped,
	quantity_received,
	unit_of_measure,
	item_description,
	item_id,
	item_revision,
	vendor_item_num,
	vendor_lot_num,
	uom_conversion_rate,
	shipment_line_status_code,
	source_document_code,
	po_header_id,
	po_release_id,
	po_line_id,
	po_line_location_id,
	po_distribution_id,
	requisition_line_id,
	req_distribution_id,
	routing_header_id,
	packing_slip,
	from_organization_id,
	deliver_to_person_id,
	employee_id,
	destination_type_code,
	to_organization_id,
	to_subinventory,
	locator_id,
	deliver_to_location_id,
	charge_account_id,
	transportation_account_id,
	shipment_unit_price,
	transfer_cost,
	transportation_cost,
	comments,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	reason_id,
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	destination_context,
	primary_unit_of_measure,
	excess_transport_reason,
	excess_transport_responsible,
	excess_transport_auth_num,
	asn_line_flag,
	original_asn_parent_line_id,
	original_asn_line_flag,
	vendor_cum_shipped_quantity,
	notice_unit_price,
	tax_name,
	tax_amount,
	invoice_status_code,
	cum_comparison_flag,
	container_num,
	truck_num,
	bar_code_label,
	transfer_percentage,
	mrc_shipment_unit_price,
	mrc_transfer_cost,
	mrc_transportation_cost,
	mrc_notice_unit_price,
	ship_to_location_id,
	country_of_origin_code,
	oe_order_header_id,
	oe_order_line_id,
	customer_item_num,
	cost_group_id,
	secondary_quantity_shipped,
	secondary_quantity_received,
	secondary_unit_of_measure,
	qc_grade,
	mmt_transaction_id,
	asn_lpn_id,
	amount,
	amount_received,
	job_id,
	timecard_id,
	timecard_ovn,
	osa_flag,
	requested_amount,
	material_stored_amount,
	approval_status,
	amount_shipped,
	lcm_shipment_line_id,
	unit_landed_cost,
	equipment_id,
KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from
	bec_ods_stg.rcv_shipment_lines
where kca_operation in ('INSERT','UPDATE') and (SHIPMENT_LINE_ID,kca_seq_id) in (select SHIPMENT_LINE_ID,max(kca_seq_id) from bec_ods_stg.rcv_shipment_lines 
where kca_operation in ('INSERT','UPDATE')
group by SHIPMENT_LINE_ID)
);

commit;



-- Soft delete
update bec_ods.rcv_shipment_lines set IS_DELETED_FLG = 'N';
commit;
update bec_ods.rcv_shipment_lines set IS_DELETED_FLG = 'Y'
where (SHIPMENT_LINE_ID)  in
(
select SHIPMENT_LINE_ID from bec_raw_dl_ext.rcv_shipment_lines
where (SHIPMENT_LINE_ID,KCA_SEQ_ID)
in 
(
select SHIPMENT_LINE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.rcv_shipment_lines
group by SHIPMENT_LINE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I', 
	last_refresh_date = getdate()
where
	ods_table_name = 'rcv_shipment_lines';

commit;