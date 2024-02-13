/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an AS IS BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/


begin;

DROP TABLE IF EXISTS bec_ods.FND_APPLICATION_TL;

CREATE TABLE IF NOT EXISTS bec_ods.FND_APPLICATION_TL
(


     APPLICATION_ID NUMERIC(15,0)   ENCODE az64   ,
	 LANGUAGE VARCHAR(150)   ENCODE lzo,
	 APPLICATION_NAME VARCHAR(150)   ENCODE lzo  ,
	 CREATED_BY NUMERIC(15,0)   ENCODE az64 ,
	 CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 ,
	 LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64,
	 LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 ,
	 LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64, 
	 DESCRIPTION VARCHAR(150)   ENCODE lzo, 
	 SOURCE_LANG VARCHAR(150)   ENCODE lzo  ,
	 ZD_EDITION_NAME VARCHAR(150)   ENCODE lzo ,
	 ZD_SYNC VARCHAR(150)   ENCODE lzo ,
	 KCA_OPERATION VARCHAR(10) ENCODE lzo,
     IS_DELETED_FLG VARCHAR(2) ENCODE lzo,
	 kca_seq_id NUMERIC(36,0)   ENCODE az64,
	 kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FND_APPLICATION_TL (
    APPLICATION_ID,
    LANGUAGE,
    APPLICATION_NAME,
    CREATED_BY, 
    CREATION_DATE,
    LAST_UPDATED_BY, 
    LAST_UPDATE_DATE, 
    LAST_UPDATE_LOGIN, 
    DESCRIPTION,
    SOURCE_LANG, 
    ZD_EDITION_NAME,
    ZD_SYNC,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        APPLICATION_ID,
        LANGUAGE,
        APPLICATION_NAME,
        CREATED_BY, 
        CREATION_DATE,
        LAST_UPDATED_BY, 
        LAST_UPDATE_DATE, 
        LAST_UPDATE_LOGIN, 
        DESCRIPTION,
        SOURCE_LANG, 
        ZD_EDITION_NAME,
        ZD_SYNC,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FND_APPLICATION_TL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fnd_application_tl';
	
commit;