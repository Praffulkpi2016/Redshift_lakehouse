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

delete from bec_ods.GL_SEG_VAL_NORM_HIERARCHY
where (flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE) in (
select stg.flex_value_set_id,stg.PARENT_FLEX_VALUE,stg.CHILD_FLEX_VALUE
from bec_ods.GL_SEG_VAL_NORM_HIERARCHY ods, bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY stg
where 
ods.flex_value_set_id = stg.flex_value_set_id and 
ods.PARENT_FLEX_VALUE = stg.PARENT_FLEX_VALUE and 
ods.CHILD_FLEX_VALUE = stg.CHILD_FLEX_VALUE  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.GL_SEG_VAL_NORM_HIERARCHY
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
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY
	where kca_operation IN ('INSERT','UPDATE') 
	and (flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,kca_seq_id) in 
	(select flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,max(kca_seq_id) from bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY 
     where kca_operation IN ('INSERT','UPDATE')
     group by flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE);

commit;

 
-- Soft delete
update bec_ods.GL_SEG_VAL_NORM_HIERARCHY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.GL_SEG_VAL_NORM_HIERARCHY set IS_DELETED_FLG = 'Y'
where (flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE)  in
(
select flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE from bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
where (flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,KCA_SEQ_ID)
in 
(
select flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
group by flex_value_set_id,PARENT_FLEX_VALUE,CHILD_FLEX_VALUE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_seg_val_norm_hierarchy';

commit;