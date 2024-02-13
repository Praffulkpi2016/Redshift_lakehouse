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

delete from bec_ods.RA_BATCH_SOURCES_ALL
where (nvl(batch_source_id,0) ,nvl(ORG_ID,0) ) in (
select nvl(stg.batch_source_id,0) ,nvl(stg.ORG_ID,0)  
from bec_ods.RA_BATCH_SOURCES_ALL ods, bec_ods_stg.RA_BATCH_SOURCES_ALL stg
where nvl(ods.batch_source_id,0) = nvl(stg.batch_source_id,0)
and nvl(ods.ORG_ID,0) = nvl(stg.ORG_ID,0) 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RA_BATCH_SOURCES_ALL
       (batch_source_id,
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
	payment_det_def_hierarchy,
    ZD_EDITION_NAME,
    zd_sync,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
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
	payment_det_def_hierarchy,
    ZD_EDITION_NAME,
    zd_sync,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RA_BATCH_SOURCES_ALL
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(batch_source_id,0),nvl(ORG_ID,0),kca_seq_id) in 
	(select nvl(batch_source_id,0),nvl(ORG_ID,0),max(kca_seq_id) from bec_ods_stg.RA_BATCH_SOURCES_ALL 
     where kca_operation in ('INSERT','UPDATE')
     group by batch_source_id,ORG_ID)
);

commit;



-- Soft delete
update bec_ods.RA_BATCH_SOURCES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_BATCH_SOURCES_ALL set IS_DELETED_FLG = 'Y'
where (nvl(batch_source_id,0),nvl(ORG_ID,0))  in
(
select nvl(batch_source_id,0),nvl(ORG_ID,0) from bec_raw_dl_ext.RA_BATCH_SOURCES_ALL
where (nvl(batch_source_id,0),nvl(ORG_ID,0),KCA_SEQ_ID)
in 
(
select nvl(batch_source_id,0),nvl(ORG_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_BATCH_SOURCES_ALL
group by nvl(batch_source_id,0),nvl(ORG_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ra_batch_sources_all';

commit;