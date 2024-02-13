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
BEGIN;
TRUNCATE TABLE bec_ods_stg.ap_payment_schedules_all;

insert into bec_ods_stg.ap_payment_schedules_all 
(
global_attribute7
,global_attribute8
,global_attribute9
,global_attribute10
,global_attribute11
,global_attribute12
,global_attribute13
,global_attribute14
,global_attribute15
,global_attribute16
,global_attribute17
,global_attribute18
,global_attribute19
,global_attribute20
,external_bank_account_id
,inv_curr_gross_amount
,checkrun_id
,dbi_events_complete_flag
,iby_hold_reason
,payment_method_code
,remittance_message1
,remittance_message2
,remittance_message3
,remit_to_supplier_name
,remit_to_supplier_id
,remit_to_supplier_site
,remit_to_supplier_site_id
,relationship_id
,invoice_id
,last_updated_by
,last_update_date
,payment_cross_rate
,payment_num
,amount_remaining
,created_by
,creation_date
,discount_date
,due_date
,future_pay_due_date
,gross_amount
,hold_flag
,last_update_login
,payment_method_lookup_code
,payment_priority
,payment_status_flag
,second_discount_date
,third_discount_date
,batch_id
,discount_amount_available
,second_disc_amt_available
,third_disc_amt_available
,attribute1
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute_category
,discount_amount_remaining
,org_id
,global_attribute_category
,global_attribute1
,global_attribute2
,global_attribute3
,global_attribute4
,global_attribute5
,global_attribute6
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
)(SELECT 
global_attribute7
,global_attribute8
,global_attribute9
,global_attribute10
,global_attribute11
,global_attribute12
,global_attribute13
,global_attribute14
,global_attribute15
,global_attribute16
,global_attribute17
,global_attribute18
,global_attribute19
,global_attribute20
,external_bank_account_id
,inv_curr_gross_amount
,checkrun_id
,dbi_events_complete_flag
,iby_hold_reason
,payment_method_code
,remittance_message1
,remittance_message2
,remittance_message3
,remit_to_supplier_name
,remit_to_supplier_id
,remit_to_supplier_site
,remit_to_supplier_site_id
,relationship_id
,invoice_id
,last_updated_by
,last_update_date
,payment_cross_rate
,payment_num
,amount_remaining
,created_by
,creation_date
,discount_date
,due_date
,future_pay_due_date
,gross_amount
,hold_flag
,last_update_login
,payment_method_lookup_code
,payment_priority
,payment_status_flag
,second_discount_date
,third_discount_date
,batch_id
,discount_amount_available
,second_disc_amt_available
,third_disc_amt_available
,attribute1
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute_category
,discount_amount_remaining
,org_id
,global_attribute_category
,global_attribute1
,global_attribute2
,global_attribute3
,global_attribute4
,global_attribute5
,global_attribute6
,KCA_OPERATION
,kca_seq_id
,kca_seq_date
 from bec_raw_dl_ext.ap_payment_schedules_all 
 where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (INVOICE_ID, PAYMENT_NUM,KCA_SEQ_ID) in 
	(select INVOICE_ID, PAYMENT_NUM,max(KCA_SEQ_ID) from bec_raw_dl_ext.ap_payment_schedules_all 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by INVOICE_ID, PAYMENT_NUM)
     and  kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='ap_payment_schedules_all')
	 );
END;