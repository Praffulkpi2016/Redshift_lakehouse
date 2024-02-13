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

truncate table bec_ods_stg.AP_BATCHES_ALL;

insert into	bec_ods_stg.ap_batches_all
(batch_id,
	batch_name,
	batch_date,
	last_update_date,
	last_updated_by,
	control_invoice_count,
	control_invoice_total,
	actual_invoice_count,
	actual_invoice_total,
	invoice_currency_code,
	payment_currency_code,
	last_update_login,
	creation_date,
	created_by,
	pay_group_lookup_code,
	batch_code_combination_id,
	terms_id,
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
	invoice_type_lookup_code,
	hold_lookup_code,
	hold_reason,
	doc_category_code,
	org_id,
	gl_date,
	payment_priority,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)

(
	select
		batch_id,
		batch_name,
		batch_date,
		last_update_date,
		last_updated_by,
		control_invoice_count,
		control_invoice_total,
		actual_invoice_count,
		actual_invoice_total,
		invoice_currency_code,
		payment_currency_code,
		last_update_login,
		creation_date,
		created_by,
		pay_group_lookup_code,
		batch_code_combination_id,
		terms_id,
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
		invoice_type_lookup_code,
		hold_lookup_code,
		hold_reason,
		doc_category_code,
		org_id,
		gl_date,
		payment_priority,
        KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.AP_BATCHES_ALL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (batch_id,kca_seq_id) in 
	(select batch_id,max(kca_seq_id) from bec_raw_dl_ext.AP_BATCHES_ALL 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by batch_id)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_batches_all')
);
end;