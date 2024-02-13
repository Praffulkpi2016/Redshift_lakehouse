/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods.FND_ID_FLEX_STRUCTURES_VL;

insert into bec_ods.FND_ID_FLEX_STRUCTURES_VL
(
	application_id,
	id_flex_code,
	id_flex_num,
	id_flex_structure_code,
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
	id_flex_structure_name,
	description,
	shorthand_prompt,
	KCA_OPERATION,
	IS_DELETED_FLG
	,KCA_SEQ_ID,
	kca_seq_date
	)
(
select
	application_id,
	id_flex_code,
	id_flex_num,
	id_flex_structure_code,
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
	id_flex_structure_name,
	description,
	shorthand_prompt,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.FND_ID_FLEX_STRUCTURES_VL
);

end;


update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='fnd_id_flex_structures_vl';
commit;