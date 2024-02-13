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

truncate table bec_ods_stg.GL_ENCUMBRANCE_TYPES;

insert into	bec_ods_stg.GL_ENCUMBRANCE_TYPES
   (
	encumbrance_type_id,
	encumbrance_type,
	enabled_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	encumbrance_type_key,
	zd_edition_name,
	zd_sync,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select		
	encumbrance_type_id,
	encumbrance_type,
	enabled_flag,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	encumbrance_type_key,
	zd_edition_name,
	zd_sync,
	kca_operation,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (ENCUMBRANCE_TYPE_ID,kca_seq_id) in 
	(select ENCUMBRANCE_TYPE_ID,max(kca_seq_id) from bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by ENCUMBRANCE_TYPE_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_encumbrance_types')
		 
            )
);
end;