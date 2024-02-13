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

DROP TABLE IF EXISTS bec_ods.GL_LEDGER_RELATIONSHIPS;

CREATE TABLE IF NOT EXISTS bec_ods.GL_LEDGER_RELATIONSHIPS
(
    RELATIONSHIP_ID NUMERIC(15,0) ENCODE az64 , 
	SOURCE_LEDGER_ID NUMERIC(15,0) ENCODE az64 , 
	TARGET_LEDGER_ID NUMERIC(15,0) ENCODE az64 , 
	SLA_LEDGER_ID NUMERIC(15,0) ENCODE az64 , 
	PRIMARY_LEDGER_ID NUMERIC(15,0) ENCODE az64 , 
	TARGET_CURRENCY_CODE VARCHAR(15) ENCODE lzo, 
	TARGET_LEDGER_NAME VARCHAR(30) ENCODE lzo, 
	TARGET_LEDGER_SHORT_NAME VARCHAR(20) ENCODE lzo, 
	TARGET_LEDGER_CATEGORY_CODE VARCHAR(30) ENCODE lzo, 
	RELATIONSHIP_TYPE_CODE VARCHAR(30) ENCODE lzo, 
	RELATIONSHIP_ENABLED_FLAG VARCHAR(1) ENCODE lzo, 
	INHERIT_CREATION_USER_FLAG VARCHAR(1) ENCODE lzo, 
	LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 , 
	LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 , 
	CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 , 
	CREATED_BY NUMERIC(15,0) ENCODE az64 , 
	LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 , 
	GL_JE_CONVERSION_SET_ID NUMERIC(15,0) ENCODE az64 , 
	SL_COA_MAPPING_ID NUMERIC(15,0) ENCODE az64 , 
	AUTOMATIC_POST_FLAG VARCHAR(1) ENCODE lzo, 
	ALC_DEFAULT_CONV_RATE_TYPE VARCHAR(30) ENCODE lzo, 
	ALC_NO_RATE_ACTION_CODE VARCHAR(30) ENCODE lzo, 
	ALC_MAX_DAYS_ROLL_RATE NUMERIC(28,10) ENCODE az64 , 
	ALC_INHERIT_CONVERSION_TYPE VARCHAR(1) ENCODE lzo, 
	ALC_INIT_CONV_OPTION_CODE VARCHAR(30) ENCODE lzo, 
	ALC_INIT_PERIOD VARCHAR(15) ENCODE lzo, 
	ALC_INIT_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
	ALC_INITIALIZING_RATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
	ALC_INITIALIZING_RATE_TYPE VARCHAR(30) ENCODE lzo, 
	ALC_PERIOD_AVERAGE_RATE_TYPE VARCHAR(30) ENCODE lzo, 
	ALC_PERIOD_END_RATE_TYPE VARCHAR(30) ENCODE lzo, 
	APPLICATION_ID NUMERIC(15,0) ENCODE az64 , 
	ORG_ID NUMERIC(15,0) ENCODE az64 , 
	DISABLE_CONVERSION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64, 
	STATUS_CODE VARCHAR(30) ENCODE lzo, 
	HIST_CONV_STATUS_CODE VARCHAR(30) ENCODE lzo,
	KCA_OPERATION VARCHAR(10) ENCODE lzo,
    IS_DELETED_FLG VARCHAR(2) ENCODE lzo,
	kca_seq_id NUMERIC(36,0)   ENCODE az64,
	kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.GL_LEDGER_RELATIONSHIPS (
 RELATIONSHIP_ID,
 SOURCE_LEDGER_ID, 
 TARGET_LEDGER_ID,
 SLA_LEDGER_ID, 
 PRIMARY_LEDGER_ID,
 TARGET_CURRENCY_CODE,
 TARGET_LEDGER_NAME, 
 TARGET_LEDGER_SHORT_NAME,
 TARGET_LEDGER_CATEGORY_CODE, 
 RELATIONSHIP_TYPE_CODE,
 RELATIONSHIP_ENABLED_FLAG,
 INHERIT_CREATION_USER_FLAG,
 LAST_UPDATE_DATE, 
 LAST_UPDATED_BY, 
 CREATION_DATE, 
 CREATED_BY, 
 LAST_UPDATE_LOGIN,
 GL_JE_CONVERSION_SET_ID, 
 SL_COA_MAPPING_ID,
 AUTOMATIC_POST_FLAG, 
 ALC_DEFAULT_CONV_RATE_TYPE,
 ALC_NO_RATE_ACTION_CODE,
 ALC_MAX_DAYS_ROLL_RATE, 
 ALC_INHERIT_CONVERSION_TYPE,
 ALC_INIT_CONV_OPTION_CODE,
 ALC_INIT_PERIOD,
 ALC_INIT_DATE, 
 ALC_INITIALIZING_RATE_DATE,
 ALC_INITIALIZING_RATE_TYPE,
 ALC_PERIOD_AVERAGE_RATE_TYPE, 
 ALC_PERIOD_END_RATE_TYPE,
 APPLICATION_ID, 
 ORG_ID, 
 DISABLE_CONVERSION_DATE,
 STATUS_CODE, 
 HIST_CONV_STATUS_CODE,
 KCA_OPERATION,
 IS_DELETED_FLG,
 kca_seq_id,
	kca_seq_date)
    SELECT
        RELATIONSHIP_ID,
        SOURCE_LEDGER_ID, 
        TARGET_LEDGER_ID,
        SLA_LEDGER_ID, 
        PRIMARY_LEDGER_ID,
        TARGET_CURRENCY_CODE,
        TARGET_LEDGER_NAME, 
        TARGET_LEDGER_SHORT_NAME,
        TARGET_LEDGER_CATEGORY_CODE, 
        RELATIONSHIP_TYPE_CODE,
        RELATIONSHIP_ENABLED_FLAG,
        INHERIT_CREATION_USER_FLAG,
        LAST_UPDATE_DATE, 
        LAST_UPDATED_BY, 
        CREATION_DATE, 
        CREATED_BY, 
        LAST_UPDATE_LOGIN,
        GL_JE_CONVERSION_SET_ID, 
        SL_COA_MAPPING_ID,
        AUTOMATIC_POST_FLAG, 
        ALC_DEFAULT_CONV_RATE_TYPE,
        ALC_NO_RATE_ACTION_CODE,
        ALC_MAX_DAYS_ROLL_RATE, 
        ALC_INHERIT_CONVERSION_TYPE,
        ALC_INIT_CONV_OPTION_CODE,
        ALC_INIT_PERIOD,
        ALC_INIT_DATE, 
        ALC_INITIALIZING_RATE_DATE,
        ALC_INITIALIZING_RATE_TYPE,
        ALC_PERIOD_AVERAGE_RATE_TYPE, 
        ALC_PERIOD_END_RATE_TYPE,
        APPLICATION_ID, 
        ORG_ID, 
        DISABLE_CONVERSION_DATE,
        STATUS_CODE, 
        HIST_CONV_STATUS_CODE,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.GL_LEDGER_RELATIONSHIPS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'gl_ledger_relationships';
	
commit;