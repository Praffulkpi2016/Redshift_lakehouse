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

drop table if exists bec_ods.ap_payment_schedules_all;

create table if not exists bec_ods.ap_payment_schedules_all
(
	global_attribute7 VARCHAR(150) ENCODE lzo
	,
global_attribute8 VARCHAR(150) ENCODE lzo
	,
global_attribute9 VARCHAR(150) ENCODE lzo
	,
global_attribute10 VARCHAR(150) ENCODE lzo
	,
global_attribute11 VARCHAR(150) ENCODE lzo
	,
global_attribute12 VARCHAR(150) ENCODE lzo
	,
global_attribute13 VARCHAR(150) ENCODE lzo
	,
global_attribute14 VARCHAR(150) ENCODE lzo
	,
global_attribute15 VARCHAR(150) ENCODE lzo
	,
global_attribute16 VARCHAR(150) ENCODE lzo
	,
global_attribute17 VARCHAR(150) ENCODE lzo
	,
global_attribute18 VARCHAR(150) ENCODE lzo
	,
global_attribute19 VARCHAR(150) ENCODE lzo
	,
global_attribute20 VARCHAR(150) ENCODE lzo
	,
external_bank_account_id NUMERIC(15,0)   ENCODE az64
	,
inv_curr_gross_amount NUMERIC(28,10)   ENCODE az64
	,
checkrun_id NUMERIC(15,0)   ENCODE az64
	,
dbi_events_complete_flag VARCHAR(1) ENCODE lzo
	,
iby_hold_reason VARCHAR(2000) ENCODE lzo
	,
payment_method_code VARCHAR(30) ENCODE lzo
	,
remittance_message1 VARCHAR(150) ENCODE lzo
	,
remittance_message2 VARCHAR(150) ENCODE lzo
	,
remittance_message3 VARCHAR(150) ENCODE lzo
	,
remit_to_supplier_name VARCHAR(240) ENCODE lzo
	,
remit_to_supplier_id NUMERIC(15,0)   ENCODE az64
	,
remit_to_supplier_site VARCHAR(240) ENCODE lzo
	,
remit_to_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,
relationship_id NUMERIC(15,0)   ENCODE az64
	,
invoice_id NUMERIC(15,0)   ENCODE az64
	,
last_updated_by NUMERIC(15,0)   ENCODE az64
	,
last_update_date TIMESTAMP without TIME zone ENCODE az64
	,
payment_cross_rate NUMERIC(28,10)   ENCODE az64
	,
payment_num NUMERIC(15,0)   ENCODE az64
	,
amount_remaining NUMERIC(28,10)   ENCODE az64
	,
created_by NUMERIC(15,0)   ENCODE az64
	,
creation_date TIMESTAMP without TIME zone ENCODE az64
	,
discount_date TIMESTAMP without TIME zone ENCODE az64
	,
due_date TIMESTAMP without TIME zone ENCODE az64
	,
future_pay_due_date TIMESTAMP without TIME zone ENCODE az64
	,
gross_amount NUMERIC(28,10)   ENCODE az64
	,
hold_flag VARCHAR(1) ENCODE lzo
	,
last_update_login NUMERIC(15,0)   ENCODE az64
	,
payment_method_lookup_code VARCHAR(25) ENCODE lzo
	,
payment_priority NUMERIC(2,0)   ENCODE az64
	,
payment_status_flag VARCHAR(25) ENCODE lzo
	,
second_discount_date TIMESTAMP without TIME zone ENCODE az64
	,
third_discount_date TIMESTAMP without TIME zone ENCODE az64
	,
batch_id NUMERIC(15,0)   ENCODE az64
	,
discount_amount_available NUMERIC(28,10)   ENCODE az64
	,
second_disc_amt_available NUMERIC(28,10)   ENCODE az64
	,
third_disc_amt_available NUMERIC(28,10)   ENCODE az64
	,
attribute1 VARCHAR(150) ENCODE lzo
	,
attribute10 VARCHAR(150) ENCODE lzo
	,
attribute11 VARCHAR(150) ENCODE lzo
	,
attribute12 VARCHAR(150) ENCODE lzo
	,
attribute13 VARCHAR(150) ENCODE lzo
	,
attribute14 VARCHAR(150) ENCODE lzo
	,
attribute15 VARCHAR(150) ENCODE lzo
	,
attribute2 VARCHAR(150) ENCODE lzo
	,
attribute3 VARCHAR(150) ENCODE lzo
	,
attribute4 VARCHAR(150) ENCODE lzo
	,
attribute5 VARCHAR(150) ENCODE lzo
	,
attribute6 VARCHAR(150) ENCODE lzo
	,
attribute7 VARCHAR(150) ENCODE lzo
	,
attribute8 VARCHAR(150) ENCODE lzo
	,
attribute9 VARCHAR(150) ENCODE lzo
	,
attribute_category VARCHAR(150) ENCODE lzo
	,
discount_amount_remaining NUMERIC(28,10)   ENCODE az64
	,
org_id NUMERIC(15,0)   ENCODE az64
	,
global_attribute_category VARCHAR(150) ENCODE lzo
	,
global_attribute1 VARCHAR(150) ENCODE lzo
	,
global_attribute2 VARCHAR(150) ENCODE lzo
	,
global_attribute3 VARCHAR(150) ENCODE lzo
	,
global_attribute4 VARCHAR(150) ENCODE lzo
	,
global_attribute5 VARCHAR(150) ENCODE lzo
	,
global_attribute6 VARCHAR(150) ENCODE lzo
,
KCA_OPERATION VARCHAR(10)   ENCODE lzo
,
IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,
kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
diststyle auto
;

insert
	into
	bec_ods.ap_payment_schedules_all
(global_attribute7,
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
	external_bank_account_id,
	inv_curr_gross_amount,
	checkrun_id,
	dbi_events_complete_flag,
	iby_hold_reason,
	payment_method_code,
	remittance_message1,
	remittance_message2,
	remittance_message3,
	remit_to_supplier_name,
	remit_to_supplier_id,
	remit_to_supplier_site,
	remit_to_supplier_site_id,
	relationship_id,
	invoice_id,
	last_updated_by,
	last_update_date,
	payment_cross_rate,
	payment_num,
	amount_remaining,
	created_by,
	creation_date,
	discount_date,
	due_date,
	future_pay_due_date,
	gross_amount,
	hold_flag,
	last_update_login,
	payment_method_lookup_code,
	payment_priority,
	payment_status_flag,
	second_discount_date,
	third_discount_date,
	batch_id,
	discount_amount_available,
	second_disc_amt_available,
	third_disc_amt_available,
	attribute1,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute_category,
	discount_amount_remaining,
	org_id,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	KCA_OPERATION,
    IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)	
(select
	global_attribute7
,
	global_attribute8
,
	global_attribute9
,
	global_attribute10
,
	global_attribute11
,
	global_attribute12
,
	global_attribute13
,
	global_attribute14
,
	global_attribute15
,
	global_attribute16
,
	global_attribute17
,
	global_attribute18
,
	global_attribute19
,
	global_attribute20
,
	external_bank_account_id
,
	inv_curr_gross_amount
,
	case
		when len(trim(CHECKRUN_ID)) = 0 then 0 :: int
		else CHECKRUN_ID :: int
	end
,
	dbi_events_complete_flag
,
	iby_hold_reason
,
	payment_method_code
,
	remittance_message1
,
	remittance_message2
,
	remittance_message3
,
	remit_to_supplier_name
,
	remit_to_supplier_id
,
	remit_to_supplier_site
,
	remit_to_supplier_site_id
,
	relationship_id
,
	invoice_id
,
	last_updated_by
,
	last_update_date :: timestamp
,
	payment_cross_rate
,
	payment_num
,
	amount_remaining
,
	created_by
,
	creation_date :: timestamp
,
	case
		when len(trim(discount_date)) = 0 then '9999-01-01' :: timestamp
		else discount_date :: timestamp
	end
,
	case
		when len(trim(due_date)) = 0 then '9999-01-01' :: timestamp
		else due_date :: timestamp
	end
,
	case
		when len(trim(future_pay_due_date)) = 0 then '9999-01-01' :: timestamp
		else future_pay_due_date :: timestamp
	end
,
	gross_amount
,
	hold_flag
,
	last_update_login
,
	payment_method_lookup_code
,
	payment_priority
,
	payment_status_flag
,
	case
		when len(trim(second_discount_date)) = 0 then '9999-01-01' :: timestamp
		else second_discount_date :: timestamp
	end
,
	case
		when len(trim(third_discount_date)) = 0 then '9999-01-01' :: timestamp
		else third_discount_date :: timestamp
	end
,
	batch_id
,
	case
		when len(trim(discount_amount_available)) = 0 then 0.0 :: numeric(3,
		1)
		else discount_amount_available :: DOUBLE precision
	end
,
	case
		when len(trim(second_disc_amt_available)) = 0 then 0.0 :: numeric(3,
		1)
		else second_disc_amt_available :: DOUBLE precision
	end
,
	case
		when len(trim(third_disc_amt_available)) = 0 then 0.0 :: numeric(3,
		1)
		else third_disc_amt_available :: DOUBLE precision
	end,
	attribute1,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute_category,
	discount_amount_remaining,
	org_id,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.ap_payment_schedules_all);
end;



update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'ap_payment_schedules_all';
	
COMMIT;