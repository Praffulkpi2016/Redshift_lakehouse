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

truncate table bec_ods_stg.GL_LE_VALUE_SETS;

insert into	bec_ods_stg.GL_LE_VALUE_SETS
   (	
	LEGAL_ENTITY_ID, 
	FLEX_VALUE_SET_ID, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	LAST_UPDATE_LOGIN, 
	CREATION_DATE, 
	CREATED_BY,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		LEGAL_ENTITY_ID, 
		FLEX_VALUE_SET_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_LOGIN, 
		CREATION_DATE, 
		CREATED_BY,
        KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.GL_LE_VALUE_SETS
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0),kca_seq_id) in 
	(select nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0),max(kca_seq_id) from bec_raw_dl_ext.GL_LE_VALUE_SETS 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by nvl(LEGAL_ENTITY_ID,0),nvl(FLEX_VALUE_SET_ID,0))
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_le_value_sets')
		 
            )
);
end;