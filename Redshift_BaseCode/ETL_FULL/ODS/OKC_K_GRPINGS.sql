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

DROP TABLE if exists bec_ods.OKC_K_GRPINGS;

CREATE TABLE IF NOT EXISTS bec_ods.OKC_K_GRPINGS
(
	ID VARCHAR(50)   ENCODE lzo 
	,CGP_PARENT_ID NUMERIC(15,0) ENCODE az64 
	,INCLUDED_CGP_ID NUMERIC(15,0) ENCODE az64 
	,INCLUDED_CHR_ID NUMERIC(15,0) ENCODE az64 
	,OBJECT_VERSION_NUMBER NUMERIC(9,0) ENCODE az64
	,CREATED_BY NUMERIC(15,0) ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64
	,SECURITY_GROUP_ID NUMERIC(15,0) ENCODE az64 
	,SCS_CODE VARCHAR(30) ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

COMMIT;

INSERT INTO bec_ods.OKC_K_GRPINGS (
	ID,
	CGP_PARENT_ID,
	INCLUDED_CGP_ID,
	INCLUDED_CHR_ID,
	OBJECT_VERSION_NUMBER,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	SECURITY_GROUP_ID,
	SCS_CODE,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		ID,
		CGP_PARENT_ID,
		INCLUDED_CGP_ID,
		INCLUDED_CHR_ID,
		OBJECT_VERSION_NUMBER,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		SECURITY_GROUP_ID,
		SCS_CODE,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.OKC_K_GRPINGS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'okc_k_grpings';
	
commit;