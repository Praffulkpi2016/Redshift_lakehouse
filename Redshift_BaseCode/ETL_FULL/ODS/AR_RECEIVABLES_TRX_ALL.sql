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

DROP TABLE if exists bec_ods.AR_RECEIVABLES_TRX_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.AR_RECEIVABLES_TRX_ALL
(
ACCOUNTING_AFFECT_FLAG	 VARCHAR(1)   ENCODE lzo
,ASSET_TAX_CODE	 VARCHAR(50)   ENCODE lzo
,ATTRIBUTE1	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	 VARCHAR(150)   ENCODE lzo
,ATTRIBUTE_CATEGORY VARCHAR(30)   ENCODE lzo
,CODE_COMBINATION_ID	NUMERIC(15,0)   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE   TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,DEFAULT_ACCTG_DISTRIBUTION_SET	NUMERIC(36,0)   ENCODE az64
,DESCRIPTION	VARCHAR(240) ENCODE lzo
,END_DATE_ACTIVE   TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,GLOBAL_ATTRIBUTE1	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE10	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE11	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE12	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE13	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE14	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE15	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE16	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE17	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE18	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE19	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE2	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE20	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE3	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE4	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE5	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE6	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE7	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE8	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE9	 VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR(30)   ENCODE lzo
,GL_ACCOUNT_SOURCE VARCHAR(30)   ENCODE lzo
,INACTIVE_DATE   TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE   TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,LIABILITY_TAX_CODE	 VARCHAR(50)   ENCODE lzo
,NAME	 VARCHAR(50)   ENCODE lzo
,ORG_ID	NUMERIC(15,0)   ENCODE az64
,RECEIVABLES_TRX_ID	NUMERIC(15,0)   ENCODE az64
,RISK_ELIMINATION_DAYS	NUMERIC(36,0)   ENCODE az64
,SET_OF_BOOKS_ID	NUMERIC(15,0)   ENCODE az64
,START_DATE_ACTIVE   TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,STATUS VARCHAR(30)   ENCODE lzo
,TAX_CODE_SOURCE VARCHAR(30)   ENCODE lzo
,TAX_RECOVERABLE_FLAG	 VARCHAR(1)   ENCODE lzo
,TYPE VARCHAR(30)   ENCODE lzo
,ZD_EDITION_NAME VARCHAR(30)   ENCODE lzo
,ZD_SYNC VARCHAR(30)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_operation VARCHAR(10)   ENCODE lzo 
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.AR_RECEIVABLES_TRX_ALL (
ACCOUNTING_AFFECT_FLAG
,ASSET_TAX_CODE
,ATTRIBUTE1
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE_CATEGORY
,CODE_COMBINATION_ID
,CREATED_BY
,CREATION_DATE
,DEFAULT_ACCTG_DISTRIBUTION_SET
,DESCRIPTION
,END_DATE_ACTIVE
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE20
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE_CATEGORY
,GL_ACCOUNT_SOURCE
,INACTIVE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATE_LOGIN
,LIABILITY_TAX_CODE
,NAME
,ORG_ID
,RECEIVABLES_TRX_ID
,RISK_ELIMINATION_DAYS
,SET_OF_BOOKS_ID
,START_DATE_ACTIVE
,STATUS
,TAX_CODE_SOURCE
,TAX_RECOVERABLE_FLAG
,TYPE
,ZD_EDITION_NAME
,ZD_SYNC
,IS_DELETED_FLG
,KCA_OPERATION
,KCA_SEQ_ID
,kca_seq_date
)
    SELECT
ACCOUNTING_AFFECT_FLAG
,ASSET_TAX_CODE
,ATTRIBUTE1
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE_CATEGORY
,CODE_COMBINATION_ID
,CREATED_BY
,CREATION_DATE
,DEFAULT_ACCTG_DISTRIBUTION_SET
,DESCRIPTION
,END_DATE_ACTIVE
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE10
,GLOBAL_ATTRIBUTE11
,GLOBAL_ATTRIBUTE12
,GLOBAL_ATTRIBUTE13
,GLOBAL_ATTRIBUTE14
,GLOBAL_ATTRIBUTE15
,GLOBAL_ATTRIBUTE16
,GLOBAL_ATTRIBUTE17
,GLOBAL_ATTRIBUTE18
,GLOBAL_ATTRIBUTE19
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE20
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
,GLOBAL_ATTRIBUTE_CATEGORY
,GL_ACCOUNT_SOURCE
,INACTIVE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATE_LOGIN
,LIABILITY_TAX_CODE
,NAME
,ORG_ID
,RECEIVABLES_TRX_ID
,RISK_ELIMINATION_DAYS
,SET_OF_BOOKS_ID
,START_DATE_ACTIVE
,STATUS
,TAX_CODE_SOURCE
,TAX_RECOVERABLE_FLAG
,TYPE
,ZD_EDITION_NAME
,ZD_SYNC
,'N' AS IS_DELETED_FLG
,KCA_OPERATION
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
,kca_seq_date
    FROM
        bec_ods_stg.AR_RECEIVABLES_TRX_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_receivables_trx_all';
	
commit;