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

DROP TABLE if exists bec_ods.CS_ESTIMATE_DETAILS;

CREATE TABLE IF NOT EXISTS bec_ods.CS_ESTIMATE_DETAILS
(
	estimate_detail_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,estimate_id NUMERIC(15,0)   ENCODE az64
	,line_number NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,serial_number VARCHAR(30)   ENCODE lzo
	,quantity_required NUMERIC(28,10)   ENCODE az64
	,unit_of_measure_code VARCHAR(3)   ENCODE lzo
	,selling_price NUMERIC(28,10)   ENCODE az64
	,after_warranty_cost NUMERIC(28,10)   ENCODE az64
	,diagnosis_id NUMERIC(15,0)   ENCODE az64
	,estimate_business_group_id NUMERIC(15,0)   ENCODE az64
	,transaction_type_id NUMERIC(15,0)   ENCODE az64
	,customer_product_id NUMERIC(15,0)   ENCODE az64
	,order_header_id NUMERIC(15,0)   ENCODE az64
	,original_system_reference VARCHAR(50)   ENCODE lzo
	,original_system_line_reference VARCHAR(50)   ENCODE lzo
	,installed_cp_return_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_cp_return_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,interface_to_oe_flag VARCHAR(1)   ENCODE lzo
	,rollup_flag VARCHAR(1)   ENCODE lzo
	,add_to_order VARCHAR(1)   ENCODE lzo
	,system_id NUMERIC(15,0)   ENCODE az64
	,rma_header_id NUMERIC(15,0)   ENCODE az64
	,rma_number NUMERIC(15,0)   ENCODE az64
	,rma_line_id NUMERIC(15,0)   ENCODE az64
	,rma_line_number NUMERIC(15,0)   ENCODE az64
	,pricing_context VARCHAR(30)   ENCODE lzo
	,pricing_attribute1 VARCHAR(150)   ENCODE lzo
	,pricing_attribute2 VARCHAR(150)   ENCODE lzo
	,pricing_attribute3 VARCHAR(150)   ENCODE lzo
	,pricing_attribute4 VARCHAR(150)   ENCODE lzo
	,pricing_attribute5 VARCHAR(150)   ENCODE lzo
	,pricing_attribute6 VARCHAR(150)   ENCODE lzo
	,pricing_attribute7 VARCHAR(150)   ENCODE lzo
	,pricing_attribute8 VARCHAR(150)   ENCODE lzo
	,pricing_attribute9 VARCHAR(150)   ENCODE lzo
	,pricing_attribute10 VARCHAR(150)   ENCODE lzo
	,pricing_attribute11 VARCHAR(150)   ENCODE lzo
	,pricing_attribute12 VARCHAR(150)   ENCODE lzo
	,pricing_attribute13 VARCHAR(150)   ENCODE lzo
	,pricing_attribute14 VARCHAR(150)   ENCODE lzo
	,pricing_attribute15 VARCHAR(150)   ENCODE lzo
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
	,context VARCHAR(30)   ENCODE lzo
	,technician_id NUMERIC(15,0)   ENCODE az64
	,txn_start_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,txn_end_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,coverage_bill_rate_id NUMERIC(15,0)   ENCODE az64
	,coverage_billing_type_id NUMERIC(15,0)   ENCODE az64
	,time_zone_id NUMERIC(15,0)   ENCODE az64
	,txn_billing_type_id NUMERIC(15,0)   ENCODE az64
	,business_process_id NUMERIC(15,0)   ENCODE az64
	,incident_id NUMERIC(15,0)   ENCODE az64
	,original_source_id NUMERIC(15,0)   ENCODE az64
	,original_source_code VARCHAR(10)   ENCODE lzo
	,source_id NUMERIC(15,0)   ENCODE az64
	,source_code VARCHAR(10)   ENCODE lzo
	,contract_id NUMERIC(15,0)   ENCODE az64
	,coverage_id NUMERIC(15,0)   ENCODE az64
	,coverage_txn_group_id NUMERIC(15,0)   ENCODE az64
	,invoice_to_org_id NUMERIC(15,0)   ENCODE az64
	,ship_to_org_id NUMERIC(15,0)   ENCODE az64
	,purchase_order_num VARCHAR(50)   ENCODE lzo
	,line_type_id NUMERIC(15,0)   ENCODE az64
	,line_category_code VARCHAR(30)   ENCODE lzo
	,currency_code VARCHAR(15)   ENCODE lzo
	,conversion_rate NUMERIC(28,10)   ENCODE az64
	,conversion_type_code VARCHAR(30)   ENCODE lzo
	,conversion_rate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,return_reason_code VARCHAR(30)   ENCODE lzo
	,order_line_id NUMERIC(15,0)   ENCODE az64
	,price_list_header_id NUMERIC(15,0)   ENCODE az64
	,func_curr_aft_warr_cost NUMERIC(28,10)   ENCODE az64
	,orig_system_reference VARCHAR(50)   ENCODE lzo
	,orig_system_line_reference VARCHAR(50)   ENCODE lzo
	,add_to_order_flag VARCHAR(1)   ENCODE lzo
	,exception_coverage_used VARCHAR(1)   ENCODE lzo
	,tax_code VARCHAR(30)   ENCODE lzo
	,est_tax_amount NUMERIC(28,10)   ENCODE az64
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,pricing_attribute16 VARCHAR(150)   ENCODE lzo
	,pricing_attribute17 VARCHAR(150)   ENCODE lzo
	,pricing_attribute18 VARCHAR(150)   ENCODE lzo
	,pricing_attribute19 VARCHAR(150)   ENCODE lzo
	,pricing_attribute20 VARCHAR(150)   ENCODE lzo
	,pricing_attribute21 VARCHAR(150)   ENCODE lzo
	,pricing_attribute22 VARCHAR(150)   ENCODE lzo
	,pricing_attribute23 VARCHAR(150)   ENCODE lzo
	,pricing_attribute24 VARCHAR(150)   ENCODE lzo
	,pricing_attribute25 VARCHAR(150)   ENCODE lzo
	,pricing_attribute26 VARCHAR(150)   ENCODE lzo
	,pricing_attribute27 VARCHAR(150)   ENCODE lzo
	,pricing_attribute28 VARCHAR(150)   ENCODE lzo
	,pricing_attribute29 VARCHAR(150)   ENCODE lzo
	,pricing_attribute30 VARCHAR(150)   ENCODE lzo
	,pricing_attribute31 VARCHAR(150)   ENCODE lzo
	,pricing_attribute32 VARCHAR(150)   ENCODE lzo
	,pricing_attribute33 VARCHAR(150)   ENCODE lzo
	,pricing_attribute34 VARCHAR(150)   ENCODE lzo
	,pricing_attribute35 VARCHAR(150)   ENCODE lzo
	,pricing_attribute36 VARCHAR(150)   ENCODE lzo
	,pricing_attribute37 VARCHAR(150)   ENCODE lzo
	,pricing_attribute38 VARCHAR(150)   ENCODE lzo
	,pricing_attribute39 VARCHAR(150)   ENCODE lzo
	,pricing_attribute40 VARCHAR(150)   ENCODE lzo
	,pricing_attribute41 VARCHAR(150)   ENCODE lzo
	,pricing_attribute42 VARCHAR(150)   ENCODE lzo
	,pricing_attribute43 VARCHAR(150)   ENCODE lzo
	,pricing_attribute44 VARCHAR(150)   ENCODE lzo
	,pricing_attribute45 VARCHAR(150)   ENCODE lzo
	,pricing_attribute46 VARCHAR(150)   ENCODE lzo
	,pricing_attribute47 VARCHAR(150)   ENCODE lzo
	,pricing_attribute48 VARCHAR(150)   ENCODE lzo
	,pricing_attribute49 VARCHAR(150)   ENCODE lzo
	,pricing_attribute50 VARCHAR(150)   ENCODE lzo
	,pricing_attribute51 VARCHAR(150)   ENCODE lzo
	,pricing_attribute52 VARCHAR(150)   ENCODE lzo
	,pricing_attribute53 VARCHAR(150)   ENCODE lzo
	,pricing_attribute54 VARCHAR(150)   ENCODE lzo
	,pricing_attribute55 VARCHAR(150)   ENCODE lzo
	,pricing_attribute56 VARCHAR(150)   ENCODE lzo
	,pricing_attribute57 VARCHAR(150)   ENCODE lzo
	,pricing_attribute58 VARCHAR(150)   ENCODE lzo
	,pricing_attribute59 VARCHAR(150)   ENCODE lzo
	,pricing_attribute61 VARCHAR(150)   ENCODE lzo
	,pricing_attribute62 VARCHAR(150)   ENCODE lzo
	,pricing_attribute63 VARCHAR(150)   ENCODE lzo
	,pricing_attribute64 VARCHAR(150)   ENCODE lzo
	,pricing_attribute65 VARCHAR(150)   ENCODE lzo
	,pricing_attribute66 VARCHAR(150)   ENCODE lzo
	,pricing_attribute67 VARCHAR(150)   ENCODE lzo
	,pricing_attribute68 VARCHAR(150)   ENCODE lzo
	,pricing_attribute69 VARCHAR(150)   ENCODE lzo
	,pricing_attribute70 VARCHAR(150)   ENCODE lzo
	,pricing_attribute71 VARCHAR(150)   ENCODE lzo
	,pricing_attribute72 VARCHAR(150)   ENCODE lzo
	,pricing_attribute73 VARCHAR(150)   ENCODE lzo
	,pricing_attribute74 VARCHAR(150)   ENCODE lzo
	,pricing_attribute75 VARCHAR(150)   ENCODE lzo
	,pricing_attribute76 VARCHAR(150)   ENCODE lzo
	,pricing_attribute77 VARCHAR(150)   ENCODE lzo
	,pricing_attribute78 VARCHAR(150)   ENCODE lzo
	,pricing_attribute79 VARCHAR(150)   ENCODE lzo
	,pricing_attribute80 VARCHAR(150)   ENCODE lzo
	,pricing_attribute81 VARCHAR(150)   ENCODE lzo
	,pricing_attribute82 VARCHAR(150)   ENCODE lzo
	,pricing_attribute83 VARCHAR(150)   ENCODE lzo
	,pricing_attribute84 VARCHAR(150)   ENCODE lzo
	,pricing_attribute85 VARCHAR(150)   ENCODE lzo
	,pricing_attribute86 VARCHAR(150)   ENCODE lzo
	,pricing_attribute87 VARCHAR(150)   ENCODE lzo
	,pricing_attribute88 VARCHAR(150)   ENCODE lzo
	,pricing_attribute89 VARCHAR(150)   ENCODE lzo
	,pricing_attribute90 VARCHAR(150)   ENCODE lzo
	,pricing_attribute91 VARCHAR(150)   ENCODE lzo
	,pricing_attribute92 VARCHAR(150)   ENCODE lzo
	,pricing_attribute93 VARCHAR(150)   ENCODE lzo
	,pricing_attribute94 VARCHAR(150)   ENCODE lzo
	,pricing_attribute95 VARCHAR(150)   ENCODE lzo
	,pricing_attribute96 VARCHAR(150)   ENCODE lzo
	,pricing_attribute97 VARCHAR(150)   ENCODE lzo
	,pricing_attribute98 VARCHAR(150)   ENCODE lzo
	,pricing_attribute99 VARCHAR(150)   ENCODE lzo
	,pricing_attribute100 VARCHAR(150)   ENCODE lzo
	,pricing_attribute60 VARCHAR(150)   ENCODE lzo
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,upgraded_status_flag VARCHAR(1)   ENCODE lzo
	,orig_system_reference_id NUMERIC(15,0)   ENCODE az64
	,no_charge_flag VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,item_revision VARCHAR(3)   ENCODE lzo
	,trans_inv_organization_id NUMERIC(15,0)   ENCODE az64
	,trans_subinventory VARCHAR(10)   ENCODE lzo
	,activity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,activity_start_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,activity_end_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,generated_by_bca_engine_flag VARCHAR(1)   ENCODE lzo
	,transaction_inventory_org NUMERIC(28,10)   ENCODE az64
	,transaction_sub_inventory VARCHAR(10)   ENCODE lzo
	,activity_start_date_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,activity_end_date_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,charge_line_type VARCHAR(30)   ENCODE lzo
	,ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,bill_to_contact_id NUMERIC(15,0)   ENCODE az64
	,ship_to_account_id NUMERIC(15,0)   ENCODE az64
	,invoice_to_account_id NUMERIC(15,0)   ENCODE az64
	,list_price NUMERIC(28,10)   ENCODE az64
	,contract_discount_amount NUMERIC(28,10)   ENCODE az64
	,bill_to_party_id NUMERIC(15,0)   ENCODE az64
	,ship_to_party_id NUMERIC(15,0)   ENCODE az64
	,submit_restriction_message VARCHAR(2000)   ENCODE lzo
	,submit_error_message VARCHAR(2000)   ENCODE lzo
	,submit_from_system VARCHAR(30)   ENCODE lzo
	,line_submitted VARCHAR(1)   ENCODE lzo
	,contract_line_id NUMERIC(15,0)   ENCODE az64
	,rate_type_code VARCHAR(40)   ENCODE lzo
	,instrument_payment_use_id NUMERIC(15,0)   ENCODE az64
	,parent_instance_id NUMERIC(15,0)   ENCODE az64
	,shipping_method VARCHAR(240)   ENCODE lzo
	,arrival_date_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cost NUMERIC(28,10)   ENCODE az64
	,available_quantity NUMERIC(28,10)   ENCODE az64
	,return_type VARCHAR(240)   ENCODE lzo
	,location_id NUMERIC(15,0)   ENCODE az64
	,shipping_distance VARCHAR(50)   ENCODE lzo
	,need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,project_task_id NUMERIC(15,0)   ENCODE az64
	,expenditure_org_id NUMERIC(15,0)   ENCODE az64
	,carrier VARCHAR(25)   ENCODE lzo
	,reservation_id NUMERIC(15,0)   ENCODE az64
	,price_adj_details VARCHAR(2000)   ENCODE lzo
	,ref_est_detail_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CS_ESTIMATE_DETAILS (
	estimate_detail_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	estimate_id,
	line_number,
	inventory_item_id,
	serial_number,
	quantity_required,
	unit_of_measure_code,
	selling_price,
	after_warranty_cost,
	diagnosis_id,
	estimate_business_group_id,
	transaction_type_id,
	customer_product_id,
	order_header_id,
	original_system_reference,
	original_system_line_reference,
	installed_cp_return_by_date,
	new_cp_return_by_date,
	interface_to_oe_flag,
	rollup_flag,
	add_to_order,
	system_id,
	rma_header_id,
	rma_number,
	rma_line_id,
	rma_line_number,
	pricing_context,
	pricing_attribute1,
	pricing_attribute2,
	pricing_attribute3,
	pricing_attribute4,
	pricing_attribute5,
	pricing_attribute6,
	pricing_attribute7,
	pricing_attribute8,
	pricing_attribute9,
	pricing_attribute10,
	pricing_attribute11,
	pricing_attribute12,
	pricing_attribute13,
	pricing_attribute14,
	pricing_attribute15,
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
	context,
	technician_id,
	txn_start_time,
	txn_end_time,
	organization_id,
	coverage_bill_rate_id,
	coverage_billing_type_id,
	time_zone_id,
	txn_billing_type_id,
	business_process_id,
	incident_id,
	original_source_id,
	original_source_code,
	source_id,
	source_code,
	contract_id,
	coverage_id,
	coverage_txn_group_id,
	invoice_to_org_id,
	ship_to_org_id,
	purchase_order_num,
	line_type_id,
	line_category_code,
	currency_code,
	conversion_rate,
	conversion_type_code,
	conversion_rate_date,
	return_reason_code,
	order_line_id,
	price_list_header_id,
	func_curr_aft_warr_cost,
	orig_system_reference,
	orig_system_line_reference,
	add_to_order_flag,
	exception_coverage_used,
	tax_code,
	est_tax_amount,
	object_version_number,
	pricing_attribute16,
	pricing_attribute17,
	pricing_attribute18,
	pricing_attribute19,
	pricing_attribute20,
	pricing_attribute21,
	pricing_attribute22,
	pricing_attribute23,
	pricing_attribute24,
	pricing_attribute25,
	pricing_attribute26,
	pricing_attribute27,
	pricing_attribute28,
	pricing_attribute29,
	pricing_attribute30,
	pricing_attribute31,
	pricing_attribute32,
	pricing_attribute33,
	pricing_attribute34,
	pricing_attribute35,
	pricing_attribute36,
	pricing_attribute37,
	pricing_attribute38,
	pricing_attribute39,
	pricing_attribute40,
	pricing_attribute41,
	pricing_attribute42,
	pricing_attribute43,
	pricing_attribute44,
	pricing_attribute45,
	pricing_attribute46,
	pricing_attribute47,
	pricing_attribute48,
	pricing_attribute49,
	pricing_attribute50,
	pricing_attribute51,
	pricing_attribute52,
	pricing_attribute53,
	pricing_attribute54,
	pricing_attribute55,
	pricing_attribute56,
	pricing_attribute57,
	pricing_attribute58,
	pricing_attribute59,
	pricing_attribute61,
	pricing_attribute62,
	pricing_attribute63,
	pricing_attribute64,
	pricing_attribute65,
	pricing_attribute66,
	pricing_attribute67,
	pricing_attribute68,
	pricing_attribute69,
	pricing_attribute70,
	pricing_attribute71,
	pricing_attribute72,
	pricing_attribute73,
	pricing_attribute74,
	pricing_attribute75,
	pricing_attribute76,
	pricing_attribute77,
	pricing_attribute78,
	pricing_attribute79,
	pricing_attribute80,
	pricing_attribute81,
	pricing_attribute82,
	pricing_attribute83,
	pricing_attribute84,
	pricing_attribute85,
	pricing_attribute86,
	pricing_attribute87,
	pricing_attribute88,
	pricing_attribute89,
	pricing_attribute90,
	pricing_attribute91,
	pricing_attribute92,
	pricing_attribute93,
	pricing_attribute94,
	pricing_attribute95,
	pricing_attribute96,
	pricing_attribute97,
	pricing_attribute98,
	pricing_attribute99,
	pricing_attribute100,
	pricing_attribute60,
	security_group_id,
	upgraded_status_flag,
	orig_system_reference_id,
	no_charge_flag,
	org_id,
	item_revision,
	trans_inv_organization_id,
	trans_subinventory,
	activity_date,
	activity_start_time,
	activity_end_time,
	generated_by_bca_engine_flag,
	transaction_inventory_org,
	transaction_sub_inventory,
	activity_start_date_time,
	activity_end_date_time,
	charge_line_type,
	ship_to_contact_id,
	bill_to_contact_id,
	ship_to_account_id,
	invoice_to_account_id,
	list_price,
	contract_discount_amount,
	bill_to_party_id,
	ship_to_party_id,
	submit_restriction_message,
	submit_error_message,
	submit_from_system,
	line_submitted,
	contract_line_id,
	rate_type_code,
	instrument_payment_use_id,
	parent_instance_id,
	shipping_method,
	arrival_date_time,
	cost,
	available_quantity,
	return_type,
	location_id,
	shipping_distance,
	need_by_date,
	project_id,
	project_task_id,
	expenditure_org_id,
	carrier,
	reservation_id,
	price_adj_details,
	ref_est_detail_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	estimate_detail_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	estimate_id,
	line_number,
	inventory_item_id,
	serial_number,
	quantity_required,
	unit_of_measure_code,
	selling_price,
	after_warranty_cost,
	diagnosis_id,
	estimate_business_group_id,
	transaction_type_id,
	customer_product_id,
	order_header_id,
	original_system_reference,
	original_system_line_reference,
	installed_cp_return_by_date,
	new_cp_return_by_date,
	interface_to_oe_flag,
	rollup_flag,
	add_to_order,
	system_id,
	rma_header_id,
	rma_number,
	rma_line_id,
	rma_line_number,
	pricing_context,
	pricing_attribute1,
	pricing_attribute2,
	pricing_attribute3,
	pricing_attribute4,
	pricing_attribute5,
	pricing_attribute6,
	pricing_attribute7,
	pricing_attribute8,
	pricing_attribute9,
	pricing_attribute10,
	pricing_attribute11,
	pricing_attribute12,
	pricing_attribute13,
	pricing_attribute14,
	pricing_attribute15,
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
	context,
	technician_id,
	txn_start_time,
	txn_end_time,
	organization_id,
	coverage_bill_rate_id,
	coverage_billing_type_id,
	time_zone_id,
	txn_billing_type_id,
	business_process_id,
	incident_id,
	original_source_id,
	original_source_code,
	source_id,
	source_code,
	contract_id,
	coverage_id,
	coverage_txn_group_id,
	invoice_to_org_id,
	ship_to_org_id,
	purchase_order_num,
	line_type_id,
	line_category_code,
	currency_code,
	conversion_rate,
	conversion_type_code,
	conversion_rate_date,
	return_reason_code,
	order_line_id,
	price_list_header_id,
	func_curr_aft_warr_cost,
	orig_system_reference,
	orig_system_line_reference,
	add_to_order_flag,
	exception_coverage_used,
	tax_code,
	est_tax_amount,
	object_version_number,
	pricing_attribute16,
	pricing_attribute17,
	pricing_attribute18,
	pricing_attribute19,
	pricing_attribute20,
	pricing_attribute21,
	pricing_attribute22,
	pricing_attribute23,
	pricing_attribute24,
	pricing_attribute25,
	pricing_attribute26,
	pricing_attribute27,
	pricing_attribute28,
	pricing_attribute29,
	pricing_attribute30,
	pricing_attribute31,
	pricing_attribute32,
	pricing_attribute33,
	pricing_attribute34,
	pricing_attribute35,
	pricing_attribute36,
	pricing_attribute37,
	pricing_attribute38,
	pricing_attribute39,
	pricing_attribute40,
	pricing_attribute41,
	pricing_attribute42,
	pricing_attribute43,
	pricing_attribute44,
	pricing_attribute45,
	pricing_attribute46,
	pricing_attribute47,
	pricing_attribute48,
	pricing_attribute49,
	pricing_attribute50,
	pricing_attribute51,
	pricing_attribute52,
	pricing_attribute53,
	pricing_attribute54,
	pricing_attribute55,
	pricing_attribute56,
	pricing_attribute57,
	pricing_attribute58,
	pricing_attribute59,
	pricing_attribute61,
	pricing_attribute62,
	pricing_attribute63,
	pricing_attribute64,
	pricing_attribute65,
	pricing_attribute66,
	pricing_attribute67,
	pricing_attribute68,
	pricing_attribute69,
	pricing_attribute70,
	pricing_attribute71,
	pricing_attribute72,
	pricing_attribute73,
	pricing_attribute74,
	pricing_attribute75,
	pricing_attribute76,
	pricing_attribute77,
	pricing_attribute78,
	pricing_attribute79,
	pricing_attribute80,
	pricing_attribute81,
	pricing_attribute82,
	pricing_attribute83,
	pricing_attribute84,
	pricing_attribute85,
	pricing_attribute86,
	pricing_attribute87,
	pricing_attribute88,
	pricing_attribute89,
	pricing_attribute90,
	pricing_attribute91,
	pricing_attribute92,
	pricing_attribute93,
	pricing_attribute94,
	pricing_attribute95,
	pricing_attribute96,
	pricing_attribute97,
	pricing_attribute98,
	pricing_attribute99,
	pricing_attribute100,
	pricing_attribute60,
	security_group_id,
	upgraded_status_flag,
	orig_system_reference_id,
	no_charge_flag,
	org_id,
	item_revision,
	trans_inv_organization_id,
	trans_subinventory,
	activity_date,
	activity_start_time,
	activity_end_time,
	generated_by_bca_engine_flag,
	transaction_inventory_org,
	transaction_sub_inventory,
	activity_start_date_time,
	activity_end_date_time,
	charge_line_type,
	ship_to_contact_id,
	bill_to_contact_id,
	ship_to_account_id,
	invoice_to_account_id,
	list_price,
	contract_discount_amount,
	bill_to_party_id,
	ship_to_party_id,
	submit_restriction_message,
	submit_error_message,
	submit_from_system,
	line_submitted,
	contract_line_id,
	rate_type_code,
	instrument_payment_use_id,
	parent_instance_id,
	shipping_method,
	arrival_date_time,
	cost,
	available_quantity,
	return_type,
	location_id,
	shipping_distance,
	need_by_date,
	project_id,
	project_task_id,
	expenditure_org_id,
	carrier,
	reservation_id,
	price_adj_details,
	ref_est_detail_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CS_ESTIMATE_DETAILS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cs_estimate_details';
	
commit;