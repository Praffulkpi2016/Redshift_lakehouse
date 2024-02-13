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

delete from bec_ods.RCV_TRANSACTIONS
where TRANSACTION_ID in (
select stg.TRANSACTION_ID
from bec_ods.RCV_TRANSACTIONS ods, bec_ods_stg.RCV_TRANSACTIONS stg
where ods.TRANSACTION_ID = stg.TRANSACTION_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RCV_TRANSACTIONS
       (transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	transaction_type,
	transaction_date,
	quantity,
	unit_of_measure,
	shipment_header_id,
	shipment_line_id,
	user_entered_flag,
	interface_source_code,
	interface_source_line_id,
	inv_transaction_id,
	source_document_code,
	destination_type_code,
	primary_quantity,
	primary_unit_of_measure,
	uom_code,
	employee_id,
	parent_transaction_id,
	po_header_id,
	po_release_id,
	po_line_id,
	po_line_location_id,
	po_distribution_id,
	po_revision_num,
	requisition_line_id,
	po_unit_price,
	currency_code,
	currency_conversion_type,
	currency_conversion_rate,
	currency_conversion_date,
	routing_header_id,
	routing_step_id,
	deliver_to_person_id,
	deliver_to_location_id,
	vendor_id,
	vendor_site_id,
	organization_id,
	subinventory,
	locator_id,
	wip_entity_id,
	wip_line_id,
	wip_repetitive_schedule_id,
	wip_operation_seq_num,
	wip_resource_seq_num,
	bom_resource_id,
	location_id,
	substitute_unordered_code,
	receipt_exception_flag,
	inspection_status_code,
	accrual_status_code,
	inspection_quality_code,
	vendor_lot_num,
	rma_reference,
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
	req_distribution_id,
	department_code,
	reason_id,
	destination_context,
	locator_attribute,
	child_inspection_flag,
	source_doc_unit_of_measure,
	source_doc_quantity,
	interface_transaction_id,
	group_id,
	movement_id,
	invoice_id,
	invoice_status_code,
	qa_collection_id,
	mrc_currency_conversion_type,
	mrc_currency_conversion_date,
	mrc_currency_conversion_rate,
	--mrc_po_unit_price,
	country_of_origin_code,
	mvt_stat_status,
	quantity_billed,
	match_flag,
	amount_billed,
	match_option,
	oe_order_header_id,
	oe_order_line_id,
	customer_id,
	customer_site_id,
	lpn_id,
	transfer_lpn_id,
	mobile_txn,
	secondary_quantity,
	secondary_unit_of_measure,
	qc_grade,
	secondary_uom_code,
	pa_addition_flag,
	consigned_flag,
	source_transaction_num,
	from_subinventory,
	from_locator_id,
	amount,
	dropship_type_code,
	lpn_group_id,
	job_id,
	timecard_id,
	timecard_ovn,
	project_id,
	task_id,
	requested_amount,
	material_stored_amount,
	replenish_order_line_id,
	lcm_shipment_line_id,
	unit_landed_cost,
	receipt_confirmation_extracted,
	lcm_adjustment_num,
	xml_document_id,
	comments#1,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
		transaction_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	transaction_type,
	transaction_date,
	quantity,
	unit_of_measure,
	shipment_header_id,
	shipment_line_id,
	user_entered_flag,
	interface_source_code,
	interface_source_line_id,
	inv_transaction_id,
	source_document_code,
	destination_type_code,
	primary_quantity,
	primary_unit_of_measure,
	uom_code,
	employee_id,
	parent_transaction_id,
	po_header_id,
	po_release_id,
	po_line_id,
	po_line_location_id,
	po_distribution_id,
	po_revision_num,
	requisition_line_id,
	po_unit_price,
	currency_code,
	currency_conversion_type,
	currency_conversion_rate,
	currency_conversion_date,
	routing_header_id,
	routing_step_id,
	deliver_to_person_id,
	deliver_to_location_id,
	vendor_id,
	vendor_site_id,
	organization_id,
	subinventory,
	locator_id,
	wip_entity_id,
	wip_line_id,
	wip_repetitive_schedule_id,
	wip_operation_seq_num,
	wip_resource_seq_num,
	bom_resource_id,
	location_id,
	substitute_unordered_code,
	receipt_exception_flag,
	inspection_status_code,
	accrual_status_code,
	inspection_quality_code,
	vendor_lot_num,
	rma_reference,
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
	req_distribution_id,
	department_code,
	reason_id,
	destination_context,
	locator_attribute,
	child_inspection_flag,
	source_doc_unit_of_measure,
	source_doc_quantity,
	interface_transaction_id,
	group_id,
	movement_id,
	invoice_id,
	invoice_status_code,
	qa_collection_id,
	mrc_currency_conversion_type,
	mrc_currency_conversion_date,
	mrc_currency_conversion_rate,
	--mrc_po_unit_price,
	country_of_origin_code,
	mvt_stat_status,
	quantity_billed,
	match_flag,
	amount_billed,
	match_option,
	oe_order_header_id,
	oe_order_line_id,
	customer_id,
	customer_site_id,
	lpn_id,
	transfer_lpn_id,
	mobile_txn,
	secondary_quantity,
	secondary_unit_of_measure,
	qc_grade,
	secondary_uom_code,
	pa_addition_flag,
	consigned_flag,
	source_transaction_num,
	from_subinventory,
	from_locator_id,
	amount,
	dropship_type_code,
	lpn_group_id,
	job_id,
	timecard_id,
	timecard_ovn,
	project_id,
	task_id,
	requested_amount,
	material_stored_amount,
	replenish_order_line_id,
	lcm_shipment_line_id,
	unit_landed_cost,
	receipt_confirmation_extracted,
	lcm_adjustment_num,
	xml_document_id,
	comments#1,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RCV_TRANSACTIONS
	where kca_operation in ('INSERT','UPDATE') 
	and (TRANSACTION_ID,kca_seq_id) in 
	(select TRANSACTION_ID,max(kca_seq_id) from bec_ods_stg.RCV_TRANSACTIONS 
     where kca_operation in ('INSERT','UPDATE')
     group by TRANSACTION_ID)
);

commit;



-- Soft delete
update bec_ods.RCV_TRANSACTIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RCV_TRANSACTIONS set IS_DELETED_FLG = 'Y'
where (TRANSACTION_ID)  in
(
select TRANSACTION_ID from bec_raw_dl_ext.RCV_TRANSACTIONS
where (TRANSACTION_ID,KCA_SEQ_ID)
in 
(
select TRANSACTION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RCV_TRANSACTIONS
group by TRANSACTION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'rcv_transactions';

commit;