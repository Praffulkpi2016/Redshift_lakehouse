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

delete from bec_ods.FND_ID_FLEX_STRUCTURES
where (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM) in (
select stg.APPLICATION_ID,stg.ID_FLEX_CODE,stg.ID_FLEX_NUM
from bec_ods.FND_ID_FLEX_STRUCTURES ods, bec_ods_stg.FND_ID_FLEX_STRUCTURES stg
where 
ods.APPLICATION_ID = stg.APPLICATION_ID and 
ods.ID_FLEX_CODE = stg.ID_FLEX_CODE and 
ods.ID_FLEX_NUM = stg.ID_FLEX_NUM  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FND_ID_FLEX_STRUCTURES
       (
		application_id,
	id_flex_code,
	id_flex_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	concatenated_segment_delimiter,
	cross_segment_validation_flag,
	dynamic_inserts_allowed_flag,
	enabled_flag,
	freeze_flex_definition_flag,
	freeze_structured_hier_flag,
	shorthand_enabled_flag,
	shorthand_length,
	structure_view_name,
	id_flex_structure_code,
	ZD_EDITION_NAME,
	ZD_SYNC,	
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	application_id,
	id_flex_code,
	id_flex_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	concatenated_segment_delimiter,
	cross_segment_validation_flag,
	dynamic_inserts_allowed_flag,
	enabled_flag,
	freeze_flex_definition_flag,
	freeze_structured_hier_flag,
	shorthand_enabled_flag,
	shorthand_length,
	structure_view_name,
	id_flex_structure_code, 
	ZD_EDITION_NAME,
	ZD_SYNC,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.FND_ID_FLEX_STRUCTURES
	where kca_operation IN ('INSERT','UPDATE') 
	and (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,kca_seq_id) in 
	(select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,max(kca_seq_id) from bec_ods_stg.FND_ID_FLEX_STRUCTURES 
     where kca_operation IN ('INSERT','UPDATE')
     group by APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM);

commit;

-- Soft delete
update bec_ods.FND_ID_FLEX_STRUCTURES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FND_ID_FLEX_STRUCTURES set IS_DELETED_FLG = 'Y'
where (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM)  in
(
select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM from bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
where (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,KCA_SEQ_ID)
in 
(
select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
group by APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fnd_id_flex_structures';

commit;