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

DROP TABLE if exists bec_ods.fa_transaction_headers;

CREATE TABLE IF NOT EXISTS bec_ods.fa_transaction_headers
(
	TRANSACTION_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,BOOK_TYPE_CODE VARCHAR(15)   ENCODE lzo
	,ASSET_ID NUMERIC(15,0)   ENCODE az64
	,TRANSACTION_TYPE_CODE VARCHAR(20)   ENCODE lzo
	,TRANSACTION_DATE_ENTERED TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,DATE_EFFECTIVE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,TRANSACTION_NAME VARCHAR(30)   ENCODE lzo
	,INVOICE_TRANSACTION_ID NUMERIC(15,0)   ENCODE az64
	,SOURCE_TRANSACTION_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,MASS_REFERENCE_ID NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
	,TRANSACTION_SUBTYPE VARCHAR(9)   ENCODE lzo
	,ATTRIBUTE1 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE2 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE3 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE4 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE5 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE6 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE7 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE8 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE9 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE10 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE11 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE12 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE13 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE14 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE15 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE_CATEGORY_CODE VARCHAR(30)   ENCODE lzo
	,TRANSACTION_KEY VARCHAR(2)   ENCODE lzo	
	,AMORTIZATION_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CALLING_INTERFACE VARCHAR(30)   ENCODE lzo	
	,MASS_TRANSACTION_ID NUMERIC(15,0)   ENCODE az64	
	,MEMBER_TRANSACTION_HEADER_ID NUMERIC(15,0)   ENCODE az64	
	,TRX_REFERENCE_ID NUMERIC(15,0)   ENCODE az64	
	,EVENT_ID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.fa_transaction_headers (
	TRANSACTION_HEADER_ID,
	BOOK_TYPE_CODE,
	ASSET_ID,
	TRANSACTION_TYPE_CODE,
	TRANSACTION_DATE_ENTERED,
	DATE_EFFECTIVE,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	TRANSACTION_NAME,
	INVOICE_TRANSACTION_ID,
	SOURCE_TRANSACTION_HEADER_ID,
	MASS_REFERENCE_ID,
	LAST_UPDATE_LOGIN,
	TRANSACTION_SUBTYPE,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
	ATTRIBUTE_CATEGORY_CODE,
	TRANSACTION_KEY,
	AMORTIZATION_START_DATE,
	CALLING_INTERFACE,
	MASS_TRANSACTION_ID,
	MEMBER_TRANSACTION_HEADER_ID,
	TRX_REFERENCE_ID,
	EVENT_ID,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		TRANSACTION_HEADER_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		TRANSACTION_TYPE_CODE,
		TRANSACTION_DATE_ENTERED,
		DATE_EFFECTIVE,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		TRANSACTION_NAME,
		INVOICE_TRANSACTION_ID,
		SOURCE_TRANSACTION_HEADER_ID,
		MASS_REFERENCE_ID,
		LAST_UPDATE_LOGIN,
		TRANSACTION_SUBTYPE,
		ATTRIBUTE1,
		ATTRIBUTE2,
		ATTRIBUTE3,
		ATTRIBUTE4,
		ATTRIBUTE5,
		ATTRIBUTE6,
		ATTRIBUTE7,
		ATTRIBUTE8,
		ATTRIBUTE9,
		ATTRIBUTE10,
		ATTRIBUTE11,
		ATTRIBUTE12,
		ATTRIBUTE13,
		ATTRIBUTE14,
		ATTRIBUTE15,
		ATTRIBUTE_CATEGORY_CODE,
		TRANSACTION_KEY,
		AMORTIZATION_START_DATE,
		CALLING_INTERFACE,
		MASS_TRANSACTION_ID,
		MEMBER_TRANSACTION_HEADER_ID,
		TRX_REFERENCE_ID,
		EVENT_ID, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.fa_transaction_headers;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_transaction_headers';
	
Commit;