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

DROP TABLE if exists bec_ods.GL_IMPORT_REFERENCES;

CREATE TABLE bec_ods.GL_IMPORT_REFERENCES (
	je_batch_id NUMERIC(15,0)   ENCODE az64
	,je_header_id NUMERIC(15,0)   ENCODE az64
	,je_line_num NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,reference_1 VARCHAR(240)   ENCODE lzo
	,reference_2 VARCHAR(240)   ENCODE lzo
	,reference_3 VARCHAR(240)   ENCODE lzo
	,reference_4 VARCHAR(240)   ENCODE lzo
	,reference_5 VARCHAR(240)   ENCODE lzo
	,reference_6 VARCHAR(240)   ENCODE lzo
	,reference_7 VARCHAR(240)   ENCODE lzo
	,reference_8 VARCHAR(240)   ENCODE lzo
	,reference_9 VARCHAR(240)   ENCODE lzo
	,reference_10 VARCHAR(240)   ENCODE lzo
	,subledger_doc_sequence_id NUMERIC(15,0)   ENCODE az64
	,subledger_doc_sequence_value NUMERIC(38,10)   ENCODE az64
	,gl_sl_link_id NUMERIC(15,0)   ENCODE az64
	,gl_sl_link_table VARCHAR(30)   ENCODE lzo,
	KCA_OPERATION VARCHAR(10)   ENCODE lzo,
    "IS_DELETED_FLG" VARCHAR(2) ENCODE lzo,
	kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE
auto;
 
INSERT INTO bec_ods.GL_IMPORT_REFERENCES (	
    je_batch_id,
	je_header_id,
	je_line_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	reference_1,
	reference_2,
	reference_3,
	reference_4,
	reference_5,
	reference_6,
	reference_7,
	reference_8,
	reference_9,
	reference_10,
	subledger_doc_sequence_id,
	subledger_doc_sequence_value,
	gl_sl_link_id,
	gl_sl_link_table,
	kca_operation,
	IS_DELETED_FLG,
    kca_seq_id,
	kca_seq_date
)
    SELECT 
        je_batch_id,
	je_header_id,
	je_line_num,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	reference_1,
	reference_2,
	reference_3,
	reference_4,
	reference_5,
	reference_6,
	reference_7,
	reference_8,
	reference_9,
	reference_10,
	subledger_doc_sequence_id,
	subledger_doc_sequence_value,
	gl_sl_link_id,
	gl_sl_link_table,
    kca_operation,
    'N' IS_DELETED_FLG  ,
     CAST(nullif(kca_seq_id, '') AS NUMERIC(36, 0)) as KCA_SEQ_ID
	 ,kca_seq_date
    FROM
        bec_ods_stg.GL_IMPORT_REFERENCES;

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_import_references';
commit;