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
	bec_ods.MTL_GENERIC_DISPOSITIONS
where
	(
	NVL(DISPOSITION_ID,0),
	NVL(ORGANIZATION_ID,0)

	) in 
	(
	select
		NVL(stg.DISPOSITION_ID,0) AS DISPOSITION_ID,
		NVL(stg.ORGANIZATION_ID,0) AS ORGANIZATION_ID
	from
		bec_ods.MTL_GENERIC_DISPOSITIONS ods,
		bec_ods_stg.MTL_GENERIC_DISPOSITIONS stg
	where
	NVL(ods.DISPOSITION_ID,0) = NVL(stg.DISPOSITION_ID,0)
	and NVL(ods.ORGANIZATION_ID,0) = NVL(stg.ORGANIZATION_ID,0)
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_GENERIC_DISPOSITIONS
    (disposition_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	disable_date,
	effective_date,
	distribution_account,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	segment11,
	segment12,
	segment13,
	segment14,
	segment15,
	segment16,
	segment17,
	segment18,
	segment19,
	segment20,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	disposition_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	disable_date,
	effective_date,
	distribution_account,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	segment8,
	segment9,
	segment10,
	segment11,
	segment12,
	segment13,
	segment14,
	segment15,
	segment16,
	segment17,
	segment18,
	segment19,
	segment20,
	summary_flag,
	enabled_flag,
	start_date_active,
	end_date_active,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_GENERIC_DISPOSITIONS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(DISPOSITION_ID,0),
		NVL(ORGANIZATION_ID,0),
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(DISPOSITION_ID,0) AS DISPOSITION_ID,
	        NVL(ORGANIZATION_ID,0) AS ORGANIZATION_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.MTL_GENERIC_DISPOSITIONS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(DISPOSITION_ID,0),
			NVL(ORGANIZATION_ID,0)
			)	
	);

commit;
 
-- Soft delete
update bec_ods.MTL_GENERIC_DISPOSITIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_GENERIC_DISPOSITIONS set IS_DELETED_FLG = 'Y'
where (NVL(DISPOSITION_ID,0),NVL(ORGANIZATION_ID,0))  in
(
select NVL(DISPOSITION_ID,0),NVL(ORGANIZATION_ID,0) from bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
where (NVL(DISPOSITION_ID,0),NVL(ORGANIZATION_ID,0),KCA_SEQ_ID)
in 
(
select NVL(DISPOSITION_ID,0),NVL(ORGANIZATION_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
group by NVL(DISPOSITION_ID,0),NVL(ORGANIZATION_ID,0)
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
	ods_table_name = 'mtl_generic_dispositions';

commit;