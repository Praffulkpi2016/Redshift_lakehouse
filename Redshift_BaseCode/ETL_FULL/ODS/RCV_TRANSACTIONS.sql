/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.RCV_TRANSACTIONS;

CREATE TABLE IF NOT EXISTS bec_ods.RCV_TRANSACTIONS
(
	transaction_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transaction_type VARCHAR(25)   ENCODE lzo
	,transaction_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,quantity NUMERIC(28,10)   ENCODE az64
	,unit_of_measure VARCHAR(25)   ENCODE lzo
	,shipment_header_id NUMERIC(15,0)   ENCODE az64
	,shipment_line_id NUMERIC(15,0)   ENCODE az64
	,user_entered_flag VARCHAR(1)   ENCODE lzo
	,interface_source_code VARCHAR(25)   ENCODE lzo
	,interface_source_line_id NUMERIC(15,0)   ENCODE az64
	,inv_transaction_id NUMERIC(15,0)   ENCODE az64
	,source_document_code VARCHAR(25)   ENCODE lzo
	,destination_type_code VARCHAR(25)   ENCODE lzo
	,primary_quantity NUMERIC(28,10)   ENCODE az64
	,primary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,uom_code VARCHAR(3)   ENCODE lzo
	,employee_id NUMERIC(9,0)   ENCODE az64
	,parent_transaction_id NUMERIC(15,0)   ENCODE az64
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,po_release_id NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,po_line_location_id NUMERIC(15,0)   ENCODE az64
	,po_distribution_id NUMERIC(15,0)   ENCODE az64
	,po_revision_num NUMERIC(15,0)   ENCODE az64
	,requisition_line_id NUMERIC(15,0)   ENCODE az64
	,po_unit_price NUMERIC(28,10)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,currency_conversion_type VARCHAR(30)   ENCODE lzo
	,currency_conversion_rate NUMERIC(28,10)   ENCODE az64
	,currency_conversion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,routing_header_id NUMERIC(15,0)   ENCODE az64
	,routing_step_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_person_id NUMERIC(9,0)   ENCODE az64
	,deliver_to_location_id NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,subinventory VARCHAR(10)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,wip_entity_id NUMERIC(15,0)   ENCODE az64
	,wip_line_id NUMERIC(15,0)   ENCODE az64
	,wip_repetitive_schedule_id NUMERIC(15,0)   ENCODE az64
	,wip_operation_seq_num NUMERIC(15,0)   ENCODE az64
	,wip_resource_seq_num NUMERIC(15,0)   ENCODE az64
	,bom_resource_id NUMERIC(15,0)   ENCODE az64
	,location_id NUMERIC(15,0)   ENCODE az64
	,substitute_unordered_code VARCHAR(25)   ENCODE lzo
	,receipt_exception_flag VARCHAR(1)   ENCODE lzo
	,inspection_status_code VARCHAR(25)   ENCODE lzo
	,accrual_status_code VARCHAR(25)   ENCODE lzo
	,inspection_quality_code VARCHAR(25)   ENCODE lzo
	,vendor_lot_num VARCHAR(30)   ENCODE lzo
	,rma_reference VARCHAR(30)   ENCODE lzo
	,comments VARCHAR(240)   ENCODE lzo
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,req_distribution_id NUMERIC(15,0)   ENCODE az64
	,department_code VARCHAR(10)   ENCODE lzo
	,reason_id NUMERIC(15,0)   ENCODE az64
	,destination_context VARCHAR(30)   ENCODE lzo
	,locator_attribute VARCHAR(150)   ENCODE lzo
	,child_inspection_flag VARCHAR(1)   ENCODE lzo
	,source_doc_unit_of_measure VARCHAR(25)   ENCODE lzo
	,source_doc_quantity NUMERIC(28,10)   ENCODE az64
	,interface_transaction_id NUMERIC(15,0)   ENCODE az64
	,group_id NUMERIC(15,0)   ENCODE az64
	,movement_id NUMERIC(15,0)   ENCODE az64
	,invoice_id NUMERIC(15,0)   ENCODE az64
	,invoice_status_code VARCHAR(25)   ENCODE lzo
	,qa_collection_id NUMERIC(15,0)   ENCODE az64
	,mrc_currency_conversion_type VARCHAR(2000)   ENCODE lzo
	,mrc_currency_conversion_date VARCHAR(2000)   ENCODE lzo
	,mrc_currency_conversion_rate VARCHAR(2000)   ENCODE lzo 
	,country_of_origin_code VARCHAR(2)   ENCODE lzo
	,mvt_stat_status VARCHAR(30)   ENCODE lzo
	,quantity_billed NUMERIC(28,10)   ENCODE az64
	,match_flag VARCHAR(1)   ENCODE lzo
	,amount_billed NUMERIC(28,10)   ENCODE az64
	,match_option VARCHAR(25)   ENCODE lzo
	,oe_order_header_id NUMERIC(15,0)   ENCODE az64
	,oe_order_line_id NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,customer_site_id NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,transfer_lpn_id NUMERIC(15,0)   ENCODE az64
	,mobile_txn VARCHAR(2)   ENCODE lzo
	,secondary_quantity NUMERIC(28,10)   ENCODE az64
	,secondary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,qc_grade VARCHAR(150)   ENCODE lzo
	,secondary_uom_code VARCHAR(3)   ENCODE lzo
	,pa_addition_flag VARCHAR(1)   ENCODE lzo
	,consigned_flag VARCHAR(1)   ENCODE lzo
	,source_transaction_num VARCHAR(25)   ENCODE lzo
	,from_subinventory VARCHAR(240)   ENCODE lzo
	,from_locator_id NUMERIC(15,0)   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,dropship_type_code NUMERIC(15,0)   ENCODE az64
	,lpn_group_id NUMERIC(15,0)   ENCODE az64
	,job_id NUMERIC(15,0)   ENCODE az64
	,timecard_id NUMERIC(15,0)   ENCODE az64
	,timecard_ovn NUMERIC(28,10)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,requested_amount NUMERIC(28,10)   ENCODE az64
	,material_stored_amount NUMERIC(28,10)   ENCODE az64
	,replenish_order_line_id NUMERIC(15,0)   ENCODE az64
	,lcm_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,unit_landed_cost NUMERIC(28,10)   ENCODE az64
	,receipt_confirmation_extracted VARCHAR(1)  ENCODE lzo
	,lcm_adjustment_num NUMERIC(15,0)   ENCODE az64	
	,xml_document_id NUMERIC(15,0)   ENCODE az64	
	,"comments#1" VARCHAR(4000)   ENCODE lzo	
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RCV_TRANSACTIONS (
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
	"comments#1",
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.RCV_TRANSACTIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'rcv_transactions';
	
COMMIT;