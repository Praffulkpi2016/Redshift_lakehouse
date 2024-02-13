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

delete from bec_ods.GL_CURRENCIES
where currency_code in (
select stg.currency_code from bec_ods.GL_CURRENCIES ods, bec_ods_stg.GL_CURRENCIES stg
where ods.currency_code = stg.currency_code  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;
  -- Insert records 
insert into bec_ods.GL_CURRENCIES
(
	currency_code,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	enabled_flag,
	currency_flag,
	description,
	issuing_territory_code,
	"precision",
	extended_precision,
	symbol,
	start_date_active,
	end_date_active,
	minimum_accountable_unit,
	context,
	iso_flag,
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
	global_attribute_category,
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
	derive_effective,
	derive_type,
	derive_factor
	,KCA_OPERATION
	,IS_DELETED_FLG
,kca_seq_id	,
	kca_seq_date
) 
select
	currency_code,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	enabled_flag,
	currency_flag,
	description,
	issuing_territory_code,
	"precision",
	extended_precision,
	symbol,
	start_date_active,
	end_date_active,
	minimum_accountable_unit,
	context,
	iso_flag,
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
	global_attribute_category,
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
	derive_effective,
	derive_type,
	derive_factor
	,KCA_OPERATION
	,'N' as IS_DELETED_FLG	
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.GL_CURRENCIES
where kca_operation IN ('INSERT','UPDATE') and (currency_code,kca_seq_id) in (select currency_code,max(kca_seq_id) from bec_ods_stg.GL_CURRENCIES 
where kca_operation IN ('INSERT','UPDATE')
group by currency_code);

commit;

 

-- Soft Delete
update bec_ods.GL_CURRENCIES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_CURRENCIES set IS_DELETED_FLG = 'Y'
where (currency_code)  in
(
select currency_code from bec_raw_dl_ext.fnd_currencies
where (currency_code,KCA_SEQ_ID)
in 
(
select currency_code,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fnd_currencies
group by currency_code
) 
and kca_operation= 'DELETE'
);
commit;
end; 
 
 update
	bec_etl_ctrl.batch_ods_info
set 
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_currencies';

commit;
