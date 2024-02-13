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
	bec_ods.FUN_SEQ_VERSIONS
where
	SEQ_VERSION_ID in (
	select
		stg.SEQ_VERSION_ID
	from
		bec_ods.FUN_SEQ_VERSIONS ods,
		bec_ods_stg.FUN_SEQ_VERSIONS stg
	where
		ods.SEQ_VERSION_ID = stg.SEQ_VERSION_ID
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FUN_SEQ_VERSIONS
       (
    seq_version_id,
	seq_header_id,
	version_name,
	header_name,
	initial_value,
	start_date,
	end_date,
	current_value,
	use_status_code,
	db_sequence_name,
	object_version_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	zd_edition_name,
	zd_sync,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
(
	select
		seq_version_id,
		seq_header_id,
		version_name,
		header_name,
		initial_value,
		start_date,
		end_date,
		current_value,
		use_status_code,
		db_sequence_name,
		object_version_number,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		zd_edition_name,
		zd_sync,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.FUN_SEQ_VERSIONS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (SEQ_VERSION_ID,
		kca_seq_id) in 
	(
		select
			SEQ_VERSION_ID,
			max(kca_seq_id)
		from
			bec_ods_stg.FUN_SEQ_VERSIONS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			SEQ_VERSION_ID)
);

commit;
 
-- Soft delete
update bec_ods.FUN_SEQ_VERSIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FUN_SEQ_VERSIONS set IS_DELETED_FLG = 'Y'
where (SEQ_VERSION_ID)  in
(
select SEQ_VERSION_ID from bec_raw_dl_ext.FUN_SEQ_VERSIONS
where (SEQ_VERSION_ID,KCA_SEQ_ID)
in 
(
select SEQ_VERSION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FUN_SEQ_VERSIONS
group by SEQ_VERSION_ID
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
	ods_table_name = 'fun_seq_versions';

commit;