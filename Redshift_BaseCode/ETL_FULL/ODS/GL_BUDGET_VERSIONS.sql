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

DROP TABLE if exists bec_ods.GL_BUDGET_VERSIONS;

CREATE TABLE IF NOT EXISTS bec_ods.GL_BUDGET_VERSIONS
(
	 budget_version_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,budget_type VARCHAR(15)   ENCODE lzo
	,budget_name VARCHAR(15)   ENCODE lzo
	,version_num VARCHAR(15)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,date_opened TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_archived TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,context VARCHAR(150)   ENCODE lzo
	,control_budget_version_id NUMERIC(15,0)   ENCODE az64
	,igi_bud_nyc_flag VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE
auto;

INSERT INTO bec_ods.GL_BUDGET_VERSIONS (
   budget_version_id,
	last_update_date,
	last_updated_by,
	budget_type,
	budget_name,
	version_num,
	status,
	date_opened,
	creation_date,
	created_by,
	last_update_login,
	description,
	date_active,
	date_archived,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	context,
	control_budget_version_id,
	igi_bud_nyc_flag,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    budget_version_id,
	last_update_date,
	last_updated_by,
	budget_type,
	budget_name,
	version_num,
	status,
	date_opened,
	creation_date,
	created_by,
	last_update_login,
	description,
	date_active,
	date_archived,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	context,
	control_budget_version_id,
	igi_bud_nyc_flag,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.GL_BUDGET_VERSIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_budget_versions'; 
	
commit;