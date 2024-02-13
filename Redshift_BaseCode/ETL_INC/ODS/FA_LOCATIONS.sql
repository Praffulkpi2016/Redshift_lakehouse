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
	bec_ods.FA_LOCATIONS
where
	NVL(LOCATION_ID, 0) in 
	(
	select
		NVL(stg.LOCATION_ID, 0) as LOCATION_ID
	from
		bec_ods.FA_LOCATIONS ods,
		bec_ods_stg.FA_LOCATIONS stg
	where
		NVL(ods.LOCATION_ID, 0) = NVL(stg.LOCATION_ID, 0)
			and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FA_LOCATIONS (
	location_id,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	last_update_date,
	last_updated_by,
	last_update_login,
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
	attribute_category_code,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	location_id,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
	last_update_date,
	last_updated_by,
	last_update_login,
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
	attribute_category_code,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_LOCATIONS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(LOCATION_ID, 0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(LOCATION_ID, 0) as LOCATION_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.FA_LOCATIONS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(LOCATION_ID, 0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.FA_LOCATIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_LOCATIONS set IS_DELETED_FLG = 'Y'
where (LOCATION_ID)  in
(
select LOCATION_ID from bec_raw_dl_ext.FA_LOCATIONS
where (LOCATION_ID,KCA_SEQ_ID)
in 
(
select LOCATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_LOCATIONS
group by LOCATION_ID
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
	ods_table_name = 'fa_locations';

commit;