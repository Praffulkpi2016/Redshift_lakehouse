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

DROP TABLE if exists bec_ods.GL_ENCUMBRANCE_TYPES;

CREATE TABLE IF NOT EXISTS bec_ods.GL_ENCUMBRANCE_TYPES
(
	encumbrance_type_id NUMERIC(15,0)   ENCODE az64
	,encumbrance_type VARCHAR(30)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,encumbrance_type_key VARCHAR(30)   ENCODE lzo
	,zd_edition_name VARCHAR(30)   ENCODE lzo
	,zd_sync VARCHAR(30)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE
auto;

INSERT INTO bec_ods.GL_ENCUMBRANCE_TYPES (
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
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.GL_ENCUMBRANCE_TYPES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_encumbrance_types';
	
COMMIT;