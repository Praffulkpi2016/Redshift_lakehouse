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

truncate
	table bec_ods_stg.GL_DAILY_CONVERSION_TYPES;

insert
	into
	bec_ods_stg.GL_DAILY_CONVERSION_TYPES
(conversion_type,
	user_conversion_type,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
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
	context,
	fem_enabled_flag,
	fem_scenario,
	fem_rate_type_code,
	fem_timeframe,
	security_flag  
	,kca_operation,
	kca_seq_id,
	kca_seq_date
)
(
	select
	conversion_type,
	user_conversion_type,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
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
	context,
	fem_enabled_flag,
	fem_scenario,
	fem_rate_type_code,
	fem_timeframe,
	 security_flag  
	,kca_operation,
	kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
and (conversion_type,kca_seq_id) in 
(select conversion_type,max(kca_seq_id) from bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by conversion_type)
and 
( kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'gl_daily_conversion_types')
 
            )
);		
		
end;