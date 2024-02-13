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

DROP TABLE if exists bec_ods.GL_LEDGER_NORM_SEG_VALS;

CREATE TABLE IF NOT EXISTS bec_ods.GL_LEDGER_NORM_SEG_VALS
(
	ledger_id NUMERIC(15,0)   ENCODE az64
	,segment_type_code VARCHAR(1)   ENCODE lzo
	,segment_value VARCHAR(25)   ENCODE lzo
	,segment_value_type_code VARCHAR(1)   ENCODE lzo
	,record_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status_code VARCHAR(1)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,legal_entity_id NUMERIC(15,0)   ENCODE az64
	,sla_sequencing_flag VARCHAR(1)   ENCODE lzo
	,context VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.GL_LEDGER_NORM_SEG_VALS (
	ledger_id,
	segment_type_code,
	segment_value,
	segment_value_type_code,
	record_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	start_date,
	end_date,
	status_code,
	request_id,
	legal_entity_id,
	sla_sequencing_flag,
	context,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
    SELECT
	ledger_id,
	segment_type_code,
	segment_value,
	segment_value_type_code,
	record_id,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	start_date,
	end_date,
	status_code,
	request_id,
	legal_entity_id,
	sla_sequencing_flag,
	context,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.GL_LEDGER_NORM_SEG_VALS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_ledger_norm_seg_vals';
	
COMMIT;