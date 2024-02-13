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

DROP TABLE if exists bec_ods.MSC_COMPANY_RELATIONSHIPS;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_COMPANY_RELATIONSHIPS
(
	RELATIONSHIP_ID	NUMERIC(15,0)   ENCODE az64
	,SUBJECT_ID NUMERIC(15,0)   ENCODE az64
	,OBJECT_ID NUMERIC(15,0)   ENCODE az64
	,RELATIONSHIP_TYPE NUMERIC(15,0)   ENCODE az64
	,START_DATE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CREATED_BY NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE auto;

INSERT INTO bec_ods.MSC_COMPANY_RELATIONSHIPS (
    RELATIONSHIP_ID,
	SUBJECT_ID,
	OBJECT_ID,
	RELATIONSHIP_TYPE,
	START_DATE,
	END_DATE,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
        RELATIONSHIP_ID,
		SUBJECT_ID,
		OBJECT_ID,
		RELATIONSHIP_TYPE,
		START_DATE,
		END_DATE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MSC_COMPANY_RELATIONSHIPS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_company_relationships';
	
commit;