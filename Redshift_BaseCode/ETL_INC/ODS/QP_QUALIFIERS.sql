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

delete
from
	bec_ods.QP_QUALIFIERS
where
	NVL(QUALIFIER_ID, 0) in 
	(
	select
		NVL(stg.QUALIFIER_ID, 0) as QUALIFIER_ID
	from
		bec_ods.QP_QUALIFIERS ods,
		bec_ods_stg.QP_QUALIFIERS stg
	where
		NVL(ods.QUALIFIER_ID, 0) = NVL(stg.QUALIFIER_ID, 0)
			and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.QP_QUALIFIERS (
	qualifier_id
    ,creation_date
    ,created_by
    ,last_update_date
    ,last_updated_by
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,last_update_login
    ,qualifier_grouping_no
    ,qualifier_context
    ,qualifier_attribute
    ,qualifier_attr_value
    ,comparison_operator_code
    ,excluder_flag
    ,qualifier_rule_id
    ,start_date_active
    ,end_date_active
    ,created_from_rule_id
    ,qualifier_precedence
    ,list_header_id
    ,list_line_id
    ,qualifier_datatype
    ,qualifier_attr_value_to
    ,context
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15
    ,active_flag
    ,list_type_code
    ,qual_attr_value_from_number
    ,qual_attr_value_to_number
    ,search_ind
    ,qualifier_group_cnt
    ,header_quals_exist_flag
    ,distinct_row_count
    ,others_group_cnt
    ,segment_id
    ,orig_sys_qualifier_ref
    ,orig_sys_header_ref
    ,orig_sys_line_ref
    ,qualify_hier_descendents_flag,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	qualifier_id
    ,creation_date
    ,created_by
    ,last_update_date
    ,last_updated_by
    ,request_id
    ,program_application_id
    ,program_id
    ,program_update_date
    ,last_update_login
    ,qualifier_grouping_no
    ,qualifier_context
    ,qualifier_attribute
    ,qualifier_attr_value
    ,comparison_operator_code
    ,excluder_flag
    ,qualifier_rule_id
    ,start_date_active
    ,end_date_active
    ,created_from_rule_id
    ,qualifier_precedence
    ,list_header_id
    ,list_line_id
    ,qualifier_datatype
    ,qualifier_attr_value_to
    ,context
    ,attribute1
    ,attribute2
    ,attribute3
    ,attribute4
    ,attribute5
    ,attribute6
    ,attribute7
    ,attribute8
    ,attribute9
    ,attribute10
    ,attribute11
    ,attribute12
    ,attribute13
    ,attribute14
    ,attribute15
    ,active_flag
    ,list_type_code
    ,qual_attr_value_from_number
    ,qual_attr_value_to_number
    ,search_ind
    ,qualifier_group_cnt
    ,header_quals_exist_flag
    ,distinct_row_count
    ,others_group_cnt
    ,segment_id
    ,orig_sys_qualifier_ref
    ,orig_sys_header_ref
    ,orig_sys_line_ref
    ,qualify_hier_descendents_flag,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.QP_QUALIFIERS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		NVL(QUALIFIER_ID, 0), 
		KCA_SEQ_ID
		) in 
	(
		select
			NVL(QUALIFIER_ID, 0) as QUALIFIER_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.QP_QUALIFIERS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(QUALIFIER_ID, 0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.QP_QUALIFIERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.QP_QUALIFIERS set IS_DELETED_FLG = 'Y'
where (QUALIFIER_ID)  in
(
select QUALIFIER_ID from bec_raw_dl_ext.QP_QUALIFIERS
where (QUALIFIER_ID,KCA_SEQ_ID)
in 
(
select QUALIFIER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.QP_QUALIFIERS
group by QUALIFIER_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;
 

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'qp_qualifiers';

commit;