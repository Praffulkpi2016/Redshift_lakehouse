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

DROP TABLE if exists bec_ods.FND_ID_FLEX_STRUCTURES;

CREATE TABLE IF NOT EXISTS bec_ods.FND_ID_FLEX_STRUCTURES
(
	application_id NUMERIC(10,0)   ENCODE az64
	,id_flex_code VARCHAR(4)   ENCODE lzo
	,id_flex_num NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(10,0)   ENCODE az64
	,concatenated_segment_delimiter VARCHAR(1)   ENCODE lzo
	,cross_segment_validation_flag VARCHAR(1)   ENCODE lzo
	,dynamic_inserts_allowed_flag VARCHAR(1)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,freeze_flex_definition_flag VARCHAR(1)   ENCODE lzo
	,freeze_structured_hier_flag VARCHAR(1)   ENCODE lzo
	,shorthand_enabled_flag VARCHAR(1)   ENCODE lzo
	,shorthand_length NUMERIC(3,0)   ENCODE az64
	,structure_view_name VARCHAR(30)   ENCODE lzo
	,id_flex_structure_code VARCHAR(30)   ENCODE lzo
	,ZD_EDITION_NAME  VARCHAR(30)   ENCODE lzo
	,ZD_SYNC VARCHAR(30)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FND_ID_FLEX_STRUCTURES (
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
	ZD_EDITION_NAME,
	ZD_SYNC,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
	ZD_EDITION_NAME,
	ZD_SYNC,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FND_ID_FLEX_STRUCTURES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fnd_id_flex_structures';
	
commit;