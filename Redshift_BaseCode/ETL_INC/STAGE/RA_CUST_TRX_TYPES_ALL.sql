/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.ra_cust_trx_types_all;

 insert
	into
	bec_ods_stg.ra_cust_trx_types_all
(	cust_trx_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	post_to_gl,
	accounting_affect_flag,
	credit_memo_type_id,
	status,
	"name",
	description,
	"type",
	default_term,
	default_printing_option,
	default_status,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	subsequent_trx_type_id,
	set_of_books_id,
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
	allow_freight_flag,
	allow_overapplication_flag,
	creation_sign,
	end_date,
	gl_id_clearing,
	gl_id_tax,
	gl_id_unbilled,
	gl_id_unearned,
	start_date,
	tax_calculation_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	natural_application_only_flag,
	org_id,
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
	rule_set_id,
	signed_flag,
	drawee_issued_flag,
	magnetic_format_code,
	format_program_id,
	gl_id_unpaid_rec,
	gl_id_remittance,
	gl_id_factor,
	allocate_tax_freight,
	legal_entity_id,
	exclude_from_late_charges,
	adj_post_to_gl,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
	SELECT
	cust_trx_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	post_to_gl,
	accounting_affect_flag,
	credit_memo_type_id,
	status,
	"name",
	description,
	"type",
	default_term,
	default_printing_option,
	default_status,
	gl_id_rev,
	gl_id_freight,
	gl_id_rec,
	subsequent_trx_type_id,
	set_of_books_id,
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
	allow_freight_flag,
	allow_overapplication_flag,
	creation_sign,
	end_date,
	gl_id_clearing,
	gl_id_tax,
	gl_id_unbilled,
	gl_id_unearned,
	start_date,
	tax_calculation_flag,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	natural_application_only_flag,
	org_id,
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
	rule_set_id,
	signed_flag,
	drawee_issued_flag,
	magnetic_format_code,
	format_program_id,
	gl_id_unpaid_rec,
	gl_id_remittance,
	gl_id_factor,
	allocate_tax_freight,
	legal_entity_id,
	exclude_from_late_charges,
	adj_post_to_gl,
	KCA_OPERATION,
	KCA_SEQ_ID
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.ra_cust_trx_types_all
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (CUST_TRX_TYPE_ID,kca_seq_id) in 
	(select CUST_TRX_TYPE_ID,max(kca_seq_id) from bec_raw_dl_ext.ra_cust_trx_types_all 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by CUST_TRX_TYPE_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ra_cust_trx_types_all')

            )
			;
end;