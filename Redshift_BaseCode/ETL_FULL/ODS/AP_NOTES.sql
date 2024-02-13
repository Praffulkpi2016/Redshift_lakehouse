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

DROP TABLE if exists bec_ods.AP_NOTES;

CREATE TABLE IF NOT EXISTS bec_ods.AP_NOTES
(
	note_id NUMERIC(15,0)   ENCODE az64
	,source_object_code VARCHAR(240)   ENCODE lzo
	,source_object_id NUMERIC(15,0)   ENCODE az64
	,note_type VARCHAR(30)   ENCODE lzo
	,notes_detail VARCHAR(16383)   ENCODE lzo
	,entered_by NUMERIC(15,0)   ENCODE az64
	,entered_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,source_lang VARCHAR(4)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64 
	,NOTE_SOURCE VARCHAR(100)   ENCODE lzo	
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO;

INSERT
	INTO
	bec_ods.AP_NOTES
	(NOTE_ID,
	SOURCE_OBJECT_CODE,
	SOURCE_OBJECT_ID,
	NOTE_TYPE,
	NOTES_DETAIL,
	ENTERED_BY,
	ENTERED_DATE,
	SOURCE_LANG,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	NOTE_SOURCE,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
	)

SELECT
	NOTE_ID,
	SOURCE_OBJECT_CODE,
	SOURCE_OBJECT_ID,
	NOTE_TYPE,
	NOTES_DETAIL,
	ENTERED_BY,
	ENTERED_DATE,
	SOURCE_LANG,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	NOTE_SOURCE,
	KCA_OPERATION
	,'N' as IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
FROM
	bec_ods_stg.AP_NOTES;

end;		
	

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_notes'; 
	
commit;