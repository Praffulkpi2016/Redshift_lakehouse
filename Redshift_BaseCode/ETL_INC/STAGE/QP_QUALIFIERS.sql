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

truncate
	table bec_ods_stg.QP_QUALIFIERS;

insert
	into
	bec_ods_stg.QP_QUALIFIERS
    (qualifier_id
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
	KCA_OPERATION,
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
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.QP_QUALIFIERS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(QUALIFIER_ID, 0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(QUALIFIER_ID, 0) as QUALIFIER_ID,
			max(KCA_SEQ_ID)
		from
			bec_raw_dl_ext.QP_QUALIFIERS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(QUALIFIER_ID, 0))
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'qp_qualifiers')
);
end;