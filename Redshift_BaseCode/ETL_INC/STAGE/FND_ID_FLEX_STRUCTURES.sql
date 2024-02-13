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

truncate table bec_ods_stg.FND_ID_FLEX_STRUCTURES;

insert into	bec_ods_stg.FND_ID_FLEX_STRUCTURES
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
--	security_group_id,
	 ZD_EDITION_NAME,
	 ZD_SYNC,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select		
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
--	security_group_id,
	 ZD_EDITION_NAME,
	 ZD_SYNC,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,kca_seq_id) in 
	(select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,max(kca_seq_id) from bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM)
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fnd_id_flex_structures')	
);
end;