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

DROP TABLE if exists bec_ods.RA_BATCH_SOURCES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.RA_BATCH_SOURCES_ALL
(
	batch_source_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(50)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,last_batch_num NUMERIC(15,0)   ENCODE az64
	,default_inv_trx_type NUMERIC(15,0)   ENCODE az64
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
	,accounting_flexfield_rule VARCHAR(30)   ENCODE lzo
	,accounting_rule_rule VARCHAR(30)   ENCODE lzo
	,agreement_rule VARCHAR(30)   ENCODE lzo
	,auto_batch_numbering_flag VARCHAR(1)   ENCODE lzo
	,auto_trx_numbering_flag VARCHAR(1)   ENCODE lzo
	,batch_source_type VARCHAR(30)   ENCODE lzo
	,bill_address_rule VARCHAR(30)   ENCODE lzo
	,bill_contact_rule VARCHAR(30)   ENCODE lzo
	,bill_customer_rule VARCHAR(30)   ENCODE lzo
	,create_clearing_flag VARCHAR(1)   ENCODE lzo
	,cust_trx_type_rule VARCHAR(30)   ENCODE lzo
	,derive_date_flag VARCHAR(1)   ENCODE lzo
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,fob_point_rule VARCHAR(30)   ENCODE lzo
	,gl_date_period_rule VARCHAR(30)   ENCODE lzo
	,invalid_lines_rule VARCHAR(30)   ENCODE lzo
	,invalid_tax_rate_rule VARCHAR(30)   ENCODE lzo
	,inventory_item_rule VARCHAR(30)   ENCODE lzo
	,invoicing_rule_rule VARCHAR(30)   ENCODE lzo
	,memo_reason_rule VARCHAR(30)   ENCODE lzo
	,rev_acc_allocation_rule VARCHAR(30)   ENCODE lzo
	,salesperson_rule VARCHAR(30)   ENCODE lzo
	,sales_credit_rule VARCHAR(30)   ENCODE lzo
	,sales_credit_type_rule VARCHAR(30)   ENCODE lzo
	,sales_territory_rule VARCHAR(30)   ENCODE lzo
	,ship_address_rule VARCHAR(30)   ENCODE lzo
	,ship_contact_rule VARCHAR(30)   ENCODE lzo
	,ship_customer_rule VARCHAR(30)   ENCODE lzo
	,ship_via_rule VARCHAR(30)   ENCODE lzo
	,sold_customer_rule VARCHAR(30)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,term_rule VARCHAR(30)   ENCODE lzo
	,unit_of_measure_rule VARCHAR(30)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,customer_bank_account_rule VARCHAR(30)   ENCODE lzo
	,memo_line_rule VARCHAR(30)   ENCODE lzo
	,receipt_method_rule VARCHAR(30)   ENCODE lzo
	,related_document_rule VARCHAR(30)   ENCODE lzo
	,allow_sales_credit_flag VARCHAR(1)   ENCODE lzo
	,grouping_rule_id NUMERIC(15,0)   ENCODE az64
	,credit_memo_batch_source_id NUMERIC(15,0)   ENCODE az64
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,copy_doc_number_flag VARCHAR(1)   ENCODE lzo
	,default_reference VARCHAR(80)   ENCODE lzo
	,copy_inv_tidff_to_cm_flag VARCHAR(1)   ENCODE lzo
	,receipt_handling_option VARCHAR(30)   ENCODE lzo
	,allow_duplicate_trx_num_flag VARCHAR(1)   ENCODE lzo
	,legal_entity_id NUMERIC(15,0)   ENCODE az64
	,gen_line_level_bal_flag VARCHAR(1)   ENCODE lzo 
	,payment_det_def_hierarchy VARCHAR(20)   ENCODE lzo
	,zd_edition_name VARCHAR(30)   ENCODE lzo
	,zd_sync VARCHAR(30)   ENCODE lzo	
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RA_BATCH_SOURCES_ALL (
    batch_source_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	"name",
	org_id,
	description,
	status,
	last_batch_num,
	default_inv_trx_type,
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
	accounting_flexfield_rule,
	accounting_rule_rule,
	agreement_rule,
	auto_batch_numbering_flag,
	auto_trx_numbering_flag,
	batch_source_type,
	bill_address_rule,
	bill_contact_rule,
	bill_customer_rule,
	create_clearing_flag,
	cust_trx_type_rule,
	derive_date_flag,
	end_date,
	fob_point_rule,
	gl_date_period_rule,
	invalid_lines_rule,
	invalid_tax_rate_rule,
	inventory_item_rule,
	invoicing_rule_rule,
	memo_reason_rule,
	rev_acc_allocation_rule,
	salesperson_rule,
	sales_credit_rule,
	sales_credit_type_rule,
	sales_territory_rule,
	ship_address_rule,
	ship_contact_rule,
	ship_customer_rule,
	ship_via_rule,
	sold_customer_rule,
	start_date,
	term_rule,
	unit_of_measure_rule,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	customer_bank_account_rule,
	memo_line_rule,
	receipt_method_rule,
	related_document_rule,
	allow_sales_credit_flag,
	grouping_rule_id,
	credit_memo_batch_source_id,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	global_attribute_category,
	copy_doc_number_flag,
	default_reference,
	copy_inv_tidff_to_cm_flag,
	receipt_handling_option,
	allow_duplicate_trx_num_flag,
	legal_entity_id,
	gen_line_level_bal_flag, 
	PAYMENT_DET_DEF_HIERARCHY,
	ZD_EDITION_NAME,
	ZD_SYNC,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
        batch_source_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	"name",
	org_id,
	description,
	status,
	last_batch_num,
	default_inv_trx_type,
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
	accounting_flexfield_rule,
	accounting_rule_rule,
	agreement_rule,
	auto_batch_numbering_flag,
	auto_trx_numbering_flag,
	batch_source_type,
	bill_address_rule,
	bill_contact_rule,
	bill_customer_rule,
	create_clearing_flag,
	cust_trx_type_rule,
	derive_date_flag,
	end_date,
	fob_point_rule,
	gl_date_period_rule,
	invalid_lines_rule,
	invalid_tax_rate_rule,
	inventory_item_rule,
	invoicing_rule_rule,
	memo_reason_rule,
	rev_acc_allocation_rule,
	salesperson_rule,
	sales_credit_rule,
	sales_credit_type_rule,
	sales_territory_rule,
	ship_address_rule,
	ship_contact_rule,
	ship_customer_rule,
	ship_via_rule,
	sold_customer_rule,
	start_date,
	term_rule,
	unit_of_measure_rule,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	customer_bank_account_rule,
	memo_line_rule,
	receipt_method_rule,
	related_document_rule,
	allow_sales_credit_flag,
	grouping_rule_id,
	credit_memo_batch_source_id,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	global_attribute_category,
	copy_doc_number_flag,
	default_reference,
	copy_inv_tidff_to_cm_flag,
	receipt_handling_option,
	allow_duplicate_trx_num_flag,
	legal_entity_id,
	gen_line_level_bal_flag, 
	PAYMENT_DET_DEF_HIERARCHY,
	ZD_EDITION_NAME,
	ZD_SYNC,	
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.RA_BATCH_SOURCES_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_batch_sources_all';
	
commit;