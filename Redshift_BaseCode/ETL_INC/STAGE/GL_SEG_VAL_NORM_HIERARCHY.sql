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

truncate table bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY;

insert into	bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY
   (
	flex_value_set_id,
	parent_flex_value,
	child_flex_value,
	summary_flag,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	status_code,
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select		
	flex_value_set_id,
	parent_flex_value,
	child_flex_value,
	summary_flag,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	status_code,
	kca_operation,
	kca_seq_id,
	kca_seq_date from bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,kca_seq_id) in 
	(select flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,max(kca_seq_id) from bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_seg_val_norm_hierarchy')
		 
            )
);
end;