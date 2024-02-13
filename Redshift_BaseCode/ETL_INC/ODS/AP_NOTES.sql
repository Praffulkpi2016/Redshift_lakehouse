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

delete from bec_ods.ap_notes
where NOTE_ID in (
select stg.NOTE_ID from bec_ods.ap_notes ods, bec_ods_stg.ap_notes stg
where ods.NOTE_ID = stg.NOTE_ID and stg.kca_operation in ('INSERT', 'UPDATE'));

commit;

-- Insert records

insert into bec_ods.AP_NOTES
(     NOTE_ID,
	SOURCE_OBJECT_CODE,
	SOURCE_OBJECT_ID,
	NOTE_TYPE,
	NOTES_DETAIL,
	ENTERED_BY,
	ENTERED_DATE,
	SOURCE_LANG,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	NOTE_SOURCE,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
	)	
(
select
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
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_notes
where kca_operation IN ('INSERT','UPDATE') and (NOTE_ID,kca_seq_id) in (select NOTE_ID,max(kca_seq_id) from bec_ods_stg.ap_notes 
where kca_operation IN ('INSERT','UPDATE')
group by NOTE_ID)
);
commit;

-- Soft delete
update bec_ods.ap_notes set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_notes set IS_DELETED_FLG = 'Y'
where (NOTE_ID)  in
(
select NOTE_ID from bec_raw_dl_ext.ap_notes
where (NOTE_ID,KCA_SEQ_ID)
in 
(
select NOTE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_notes
group by NOTE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'ap_notes';

commit;