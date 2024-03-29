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

truncate table bec_ods_stg.AR_RECEIPT_METHODS;

insert into	bec_ods_stg.AR_RECEIPT_METHODS
   (receipt_method_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	"name",
	receipt_class_id,
	start_date,
	auto_print_program_id,
	auto_trans_program_id,
	end_date,
	last_update_login,
	lead_days,
	maturity_date_rule_code,
	receipt_creation_rule_code,
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
	printed_name,
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
	payment_type_code,
	merchant_id,
	receipt_inherit_inv_num_flag,
	dm_inherit_receipt_num_flag,
	br_cust_trx_type_id,
	br_min_acctd_amount,
	br_max_acctd_amount,
	br_inherit_inv_num_flag,
	merchant_ref,
	payment_channel_code,
	zd_edition_name,
	zd_sync,	
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		receipt_method_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	"name",
	receipt_class_id,
	start_date,
	auto_print_program_id,
	auto_trans_program_id,
	end_date,
	last_update_login,
	lead_days,
	maturity_date_rule_code,
	receipt_creation_rule_code,
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
	printed_name,
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
	payment_type_code,
	merchant_id,
	receipt_inherit_inv_num_flag,
	dm_inherit_receipt_num_flag,
	br_cust_trx_type_id,
	br_min_acctd_amount,
	br_max_acctd_amount,
	br_inherit_inv_num_flag,
	merchant_ref,
	payment_channel_code,
	zd_edition_name,
	zd_sync,	
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.AR_RECEIPT_METHODS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (RECEIPT_METHOD_ID,kca_seq_id) in 
	(select RECEIPT_METHOD_ID,max(kca_seq_id) from bec_raw_dl_ext.AR_RECEIPT_METHODS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by RECEIPT_METHOD_ID)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ar_receipt_methods')
);
end;