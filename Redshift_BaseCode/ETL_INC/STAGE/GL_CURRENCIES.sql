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
TRUNCATE TABLE bec_ods_stg.GL_CURRENCIES;

insert into bec_ods_stg.GL_CURRENCIES
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
	,kca_operation
	,kca_seq_id,
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
	,kca_operation
	,kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.fnd_currencies
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (CURRENCY_CODE,kca_seq_id) in  (select CURRENCY_CODE,max(kca_seq_id) from bec_raw_dl_ext.FND_CURRENCIES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by CURRENCY_CODE) and ( kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='gl_currencies')
 
            )
;
 

end;

