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

delete
from
	bec_ods.GL_DAILY_CONVERSION_TYPES
where
	conversion_type in (
	select
		stg.conversion_type
	from
		bec_ods.GL_DAILY_CONVERSION_TYPES ods,
		bec_ods_stg.GL_DAILY_CONVERSION_TYPES stg
	where
		ods.conversion_type = stg.conversion_type
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.GL_DAILY_CONVERSION_TYPES
       (
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
	is_deleted_flg,
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
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.GL_DAILY_CONVERSION_TYPES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (conversion_type,kca_seq_id) in 
	(
		select
			conversion_type,max(kca_seq_id)
		from
			bec_ods_stg.GL_DAILY_CONVERSION_TYPES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			conversion_type)
);

commit;
 
-- Soft delete
update bec_ods.GL_DAILY_CONVERSION_TYPES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_DAILY_CONVERSION_TYPES set IS_DELETED_FLG = 'Y'
where (conversion_type)  in
(
select conversion_type from bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
where (conversion_type,KCA_SEQ_ID)
in 
(
select conversion_type,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
group by conversion_type
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
	ods_table_name = 'gl_daily_conversion_types';

commit;