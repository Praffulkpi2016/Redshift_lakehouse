/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.GL_SEG_VAL_NORM_HIERARCHY;

CREATE TABLE IF NOT EXISTS bec_ods.GL_SEG_VAL_NORM_HIERARCHY
(
	flex_value_set_id NUMERIC(10,0)   ENCODE az64
	,parent_flex_value VARCHAR(60)   ENCODE lzo
	,child_flex_value VARCHAR(60)   ENCODE lzo
	,summary_flag VARCHAR(1)   ENCODE lzo
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status_code VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.GL_SEG_VAL_NORM_HIERARCHY (
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
	kca_seq_date)
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
    FROM
        bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_seg_val_norm_hierarchy';
	
commit;