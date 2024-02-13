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

truncate table bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL;

insert into	bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL
   (	
	ext_payee_id,
	payee_party_id,
	payment_function,
	exclusive_payment_flag,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	object_version_number,
	party_site_id,
	supplier_site_id,
	org_id,
	org_type,
	default_payment_method_code,
	ece_tp_location_code,
	bank_charge_bearer,
	bank_instruction1_code,
	bank_instruction2_code,
	bank_instruction_details,
	payment_reason_code,
	payment_reason_comments,
	inactive_date,
	payment_text_message1,
	payment_text_message2,
	payment_text_message3,
	delivery_channel_code,
	payment_format_code,
	settlement_priority,
	remit_advice_delivery_method,
	remit_advice_email,
	remit_advice_fax,
	service_level_code,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
	ext_payee_id,
	payee_party_id,
	payment_function,
	exclusive_payment_flag,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	object_version_number,
	party_site_id,
	supplier_site_id,
	org_id,
	org_type,
	default_payment_method_code,
	ece_tp_location_code,
	bank_charge_bearer,
	bank_instruction1_code,
	bank_instruction2_code,
	bank_instruction_details,
	payment_reason_code,
	payment_reason_comments,
	inactive_date,
	payment_text_message1,
	payment_text_message2,
	payment_text_message3,
	delivery_channel_code,
	payment_format_code,
	settlement_priority,
	remit_advice_delivery_method,
	remit_advice_email,
	remit_advice_fax,
	service_level_code,
        KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(EXT_PAYEE_ID,0),kca_seq_id) in 
	(select nvl(EXT_PAYEE_ID,0) as EXT_PAYEE_ID,max(kca_seq_id) from bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(EXT_PAYEE_ID,0))
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'iby_external_payees_all')
							 )
);
end;