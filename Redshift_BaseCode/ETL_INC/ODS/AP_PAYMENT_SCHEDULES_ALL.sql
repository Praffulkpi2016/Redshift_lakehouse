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
BEGIN;

-- Delete Records

delete from bec_ods.AP_PAYMENT_SCHEDULES_ALL
where (INVOICE_ID,PAYMENT_NUM) in (
select stg.INVOICE_ID,stg.PAYMENT_NUM 
from bec_ods.AP_PAYMENT_SCHEDULES_ALL ods, bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL stg
where ods.INVOICE_ID = stg.INVOICE_ID
and ods.PAYMENT_NUM = stg.PAYMENT_NUM
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.AP_PAYMENT_SCHEDULES_ALL
(global_attribute7
, global_attribute8
, global_attribute9
, global_attribute10
, global_attribute11
, global_attribute12
, global_attribute13
, global_attribute14
, global_attribute15
, global_attribute16
, global_attribute17
, global_attribute18
, global_attribute19
, global_attribute20
, external_bank_account_id
, inv_curr_gross_amount
, checkrun_id
, dbi_events_complete_flag
, iby_hold_reason
, payment_method_code
, remittance_message1
, remittance_message2
, remittance_message3
, remit_to_supplier_name
, remit_to_supplier_id
, remit_to_supplier_site
, remit_to_supplier_site_id
, relationship_id
, invoice_id
, last_updated_by
, last_update_date
, payment_cross_rate
, payment_num
, amount_remaining
, created_by
, creation_date
, discount_date
, due_date
, future_pay_due_date
, gross_amount
, hold_flag
, last_update_login
, payment_method_lookup_code
, payment_priority
, payment_status_flag
, second_discount_date
, third_discount_date
, batch_id
, discount_amount_available
, second_disc_amt_available
, third_disc_amt_available
, attribute1
, attribute10
, attribute11
, attribute12
, attribute13
, attribute14
, attribute15
, attribute2
, attribute3
, attribute4
, attribute5
, attribute6
, attribute7
, attribute8
, attribute9
, attribute_category
, discount_amount_remaining
, org_id
, global_attribute_category
, global_attribute1
, global_attribute2
, global_attribute3
, global_attribute4
, global_attribute5
, global_attribute6
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
   global_attribute7
, global_attribute8
, global_attribute9
, global_attribute10
, global_attribute11
, global_attribute12
, global_attribute13
, global_attribute14
, global_attribute15
, global_attribute16
, global_attribute17
, global_attribute18
, global_attribute19
, global_attribute20
, external_bank_account_id
, inv_curr_gross_amount
, checkrun_id
, dbi_events_complete_flag
, iby_hold_reason
, payment_method_code
, remittance_message1
, remittance_message2
, remittance_message3
, remit_to_supplier_name
, remit_to_supplier_id
, remit_to_supplier_site
, remit_to_supplier_site_id
, relationship_id
, invoice_id
, last_updated_by
, last_update_date
, payment_cross_rate
, payment_num
, amount_remaining
, created_by
, creation_date
, discount_date
, due_date
, future_pay_due_date
, gross_amount
, hold_flag
, last_update_login
, payment_method_lookup_code
, payment_priority
, payment_status_flag
, second_discount_date
, third_discount_date
, batch_id
, discount_amount_available
, second_disc_amt_available
, third_disc_amt_available
, attribute1
, attribute10
, attribute11
, attribute12
, attribute13
, attribute14
, attribute15
, attribute2
, attribute3
, attribute4
, attribute5
, attribute6
, attribute7
, attribute8
, attribute9
, attribute_category
, discount_amount_remaining
, org_id
, global_attribute_category
, global_attribute1
, global_attribute2
, global_attribute3
, global_attribute4
, global_attribute5
, global_attribute6
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL
where kca_operation IN ('INSERT','UPDATE') 
	and (INVOICE_ID, PAYMENT_NUM,kca_seq_id) in 
	(select INVOICE_ID, PAYMENT_NUM,max(kca_seq_id) from bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by INVOICE_ID, PAYMENT_NUM)
);

commit;

-- Soft delete
update bec_ods.AP_PAYMENT_SCHEDULES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AP_PAYMENT_SCHEDULES_ALL set IS_DELETED_FLG = 'Y'
where (INVOICE_ID, PAYMENT_NUM)  in
(
select INVOICE_ID, PAYMENT_NUM from bec_raw_dl_ext.AP_PAYMENT_SCHEDULES_ALL
where (INVOICE_ID, PAYMENT_NUM,KCA_SEQ_ID)
in 
(
select INVOICE_ID, PAYMENT_NUM,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AP_PAYMENT_SCHEDULES_ALL
group by INVOICE_ID, PAYMENT_NUM
) 
and kca_operation= 'DELETE'
);
commit;


end;

update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='ap_payment_schedules_all';
commit;