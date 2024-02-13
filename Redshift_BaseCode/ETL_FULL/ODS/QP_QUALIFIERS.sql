/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach FOR ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.QP_QUALIFIERS;

CREATE TABLE IF NOT EXISTS bec_ods.QP_QUALIFIERS
(

	qualifier_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,qualifier_grouping_no NUMERIC(15,0)   ENCODE az64
	,qualifier_context VARCHAR(240)   ENCODE lzo
    ,qualifier_attribute VARCHAR(30)   ENCODE lzo
    ,qualifier_attr_value VARCHAR(30)   ENCODE lzo
    ,comparison_operator_code VARCHAR(240)   ENCODE lzo
    ,excluder_flag VARCHAR(1)   ENCODE lzo
    ,qualifier_rule_id NUMERIC(15,0)   ENCODE az64
    ,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
    ,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
    ,created_from_rule_id NUMERIC(15,0)   ENCODE az64
    ,qualifier_precedence NUMERIC(15,0)   ENCODE az64
    ,list_header_id NUMERIC(15,0)   ENCODE az64
    ,list_line_id NUMERIC(15,0)   ENCODE az64
    ,qualifier_datatype VARCHAR(240)   ENCODE lzo
    ,qualifier_attr_value_to VARCHAR(240)   ENCODE lzo
    ,context VARCHAR(240)   ENCODE lzo
    ,attribute1 VARCHAR(240)   ENCODE lzo
    ,attribute2 VARCHAR(240)   ENCODE lzo
    ,attribute3 VARCHAR(240)   ENCODE lzo
    ,attribute4 VARCHAR(240)   ENCODE lzo
    ,attribute5 VARCHAR(240)   ENCODE lzo
    ,attribute6 VARCHAR(240)   ENCODE lzo
    ,attribute7 VARCHAR(240)   ENCODE lzo
    ,attribute8 VARCHAR(240)   ENCODE lzo
    ,attribute9 VARCHAR(240)   ENCODE lzo
    ,attribute10 VARCHAR(240)   ENCODE lzo
    ,attribute11 VARCHAR(240)   ENCODE lzo
    ,attribute12 VARCHAR(240)   ENCODE lzo
    ,attribute13 VARCHAR(240)   ENCODE lzo
    ,attribute14 VARCHAR(240)   ENCODE lzo
    ,attribute15 VARCHAR(240)   ENCODE lzo
    ,active_flag VARCHAR(1)   ENCODE lzo
    ,list_type_code VARCHAR(30)   ENCODE lzo
    ,qual_attr_value_from_number NUMERIC(15,0)   ENCODE az64
    ,qual_attr_value_to_number NUMERIC(15,0)   ENCODE az64
    ,search_ind NUMERIC(15,0)   ENCODE az64
    ,qualifier_group_cnt NUMERIC(15,0)   ENCODE az64
    ,header_quals_exist_flag VARCHAR(1)   ENCODE lzo
    ,distinct_row_count NUMERIC(15,0)   ENCODE az64
    ,others_group_cnt NUMERIC(15,0)   ENCODE az64
    ,segment_id NUMERIC(15,0)   ENCODE az64
    ,orig_sys_qualifier_ref VARCHAR(50)   ENCODE lzo
    ,orig_sys_header_ref VARCHAR(50)   ENCODE lzo
    ,orig_sys_line_ref VARCHAR(50)   ENCODE lzo
    ,qualify_hier_descendents_flag VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.QP_QUALIFIERS (
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
	kca_seq_date
)
    SELECT
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
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.QP_QUALIFIERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'qp_qualifiers';
	
COMMIT;