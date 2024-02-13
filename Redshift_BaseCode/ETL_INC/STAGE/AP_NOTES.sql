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

truncate table bec_ods_stg.AP_NOTES;
	
	insert into	bec_ods_stg.AP_NOTES
(
note_id,
	source_object_code,
	source_object_id,
	note_type,
	notes_detail,
	entered_by,
	entered_date,
	source_lang,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	NOTE_SOURCE,
	KCA_OPERATION
	,kca_seq_id
	,kca_seq_date
)
( 
SELECT
note_id,
	source_object_code,
	source_object_id,
	note_type,
	notes_detail,
	entered_by,
	entered_date,
	source_lang,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	NOTE_SOURCE,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.AP_NOTES
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' and (NOTE_ID,kca_seq_id) in (select NOTE_ID,max(kca_seq_id) from bec_raw_dl_ext.AP_NOTES 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by NOTE_ID)
and 
kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'ap_notes')
);
end;