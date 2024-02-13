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
	table bec_ods_stg.FUN_SEQ_VERSIONS;

insert
	into
	bec_ods_stg.FUN_SEQ_VERSIONS
(seq_version_id,
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
	KCA_OPERATION,
	kca_seq_id
	,kca_seq_date
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
		KCA_OPERATION,
		kca_seq_id
		,kca_seq_date
	from
		bec_raw_dl_ext.FUN_SEQ_VERSIONS
	where
		kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (SEQ_VERSION_ID,
		kca_seq_id) in 
(
		select
			SEQ_VERSION_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.FUN_SEQ_VERSIONS
		where
			kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			SEQ_VERSION_ID)
		and 
(kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fun_seq_versions')
		 
            )
);
end;