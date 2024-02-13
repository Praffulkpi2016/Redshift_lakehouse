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

DROP TABLE if exists bec_ods.fa_adjustments;

CREATE TABLE IF NOT EXISTS bec_ods.fa_adjustments
(
	TRANSACTION_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,SOURCE_TYPE_CODE VARCHAR(15)   ENCODE lzo
	,ADJUSTMENT_TYPE VARCHAR(15)   ENCODE lzo
	,DEBIT_CREDIT_FLAG VARCHAR(2)   ENCODE lzo
	,CODE_COMBINATION_ID NUMERIC(15,0)   ENCODE az64
	,BOOK_TYPE_CODE VARCHAR(15)   ENCODE lzo
	,ASSET_ID NUMERIC(15,0)   ENCODE az64
	,ADJUSTMENT_AMOUNT NUMERIC(28,10)   ENCODE az64
	,DISTRIBUTION_ID NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
	,ANNUALIZED_ADJUSTMENT NUMERIC(28,10)   ENCODE az64
	,JE_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,JE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,PERIOD_COUNTER_ADJUSTED NUMERIC(15,0)   ENCODE az64
	,PERIOD_COUNTER_CREATED NUMERIC(15,0)   ENCODE az64
	,ASSET_INVOICE_ID NUMERIC(15,0)   ENCODE az64
	,GLOBAL_ATTRIBUTE1 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE2 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE3 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE4 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE5 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE6 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE7 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE8 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE9 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE10 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE11 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE12 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE13 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE14 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE15 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE16 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE17 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE18 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE19 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE20 VARCHAR(150)   ENCODE LZO	
	,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR(30)   ENCODE lzo
	,DEPRN_OVERRIDE_FLAG VARCHAR(1)   ENCODE lzo
	,TRACK_MEMBER_FLAG VARCHAR(1)   ENCODE lzo	
	,ADJUSTMENT_LINE_ID NUMERIC(15,0)   ENCODE az64
	,SOURCE_LINE_ID NUMERIC(15,0)   ENCODE az64
	,SOURCE_DEST_CODE VARCHAR(15)   ENCODE lzo
	,INSERTION_ORDER NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.fa_adjustments (
	TRANSACTION_HEADER_ID,
	SOURCE_TYPE_CODE,
	ADJUSTMENT_TYPE,
	DEBIT_CREDIT_FLAG,
	CODE_COMBINATION_ID,
	BOOK_TYPE_CODE,
	ASSET_ID,
	ADJUSTMENT_AMOUNT,
	DISTRIBUTION_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	ANNUALIZED_ADJUSTMENT,
	JE_HEADER_ID,
	JE_LINE_NUM,
	PERIOD_COUNTER_ADJUSTED,
	PERIOD_COUNTER_CREATED,
	ASSET_INVOICE_ID,
	GLOBAL_ATTRIBUTE1,
	GLOBAL_ATTRIBUTE2,
	GLOBAL_ATTRIBUTE3,
	GLOBAL_ATTRIBUTE4,
	GLOBAL_ATTRIBUTE5,
	GLOBAL_ATTRIBUTE6,
	GLOBAL_ATTRIBUTE7,
	GLOBAL_ATTRIBUTE8,
	GLOBAL_ATTRIBUTE9,
	GLOBAL_ATTRIBUTE10,
	GLOBAL_ATTRIBUTE11,
	GLOBAL_ATTRIBUTE12,
	GLOBAL_ATTRIBUTE13,
	GLOBAL_ATTRIBUTE14,
	GLOBAL_ATTRIBUTE15,
	GLOBAL_ATTRIBUTE16,
	GLOBAL_ATTRIBUTE17,
	GLOBAL_ATTRIBUTE18,
	GLOBAL_ATTRIBUTE19,
	GLOBAL_ATTRIBUTE20,
	GLOBAL_ATTRIBUTE_CATEGORY,
	DEPRN_OVERRIDE_FLAG,
	TRACK_MEMBER_FLAG,
	ADJUSTMENT_LINE_ID,
	SOURCE_LINE_ID,
	SOURCE_DEST_CODE,
	INSERTION_ORDER,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		TRANSACTION_HEADER_ID,
		SOURCE_TYPE_CODE,
		ADJUSTMENT_TYPE,
		DEBIT_CREDIT_FLAG,
		CODE_COMBINATION_ID,
		BOOK_TYPE_CODE,
		ASSET_ID,
		ADJUSTMENT_AMOUNT,
		DISTRIBUTION_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		ANNUALIZED_ADJUSTMENT,
		JE_HEADER_ID,
		JE_LINE_NUM,
		PERIOD_COUNTER_ADJUSTED,
		PERIOD_COUNTER_CREATED,
		ASSET_INVOICE_ID,
		GLOBAL_ATTRIBUTE1,
		GLOBAL_ATTRIBUTE2,
		GLOBAL_ATTRIBUTE3,
		GLOBAL_ATTRIBUTE4,
		GLOBAL_ATTRIBUTE5,
		GLOBAL_ATTRIBUTE6,
		GLOBAL_ATTRIBUTE7,
		GLOBAL_ATTRIBUTE8,
		GLOBAL_ATTRIBUTE9,
		GLOBAL_ATTRIBUTE10,
		GLOBAL_ATTRIBUTE11,
		GLOBAL_ATTRIBUTE12,
		GLOBAL_ATTRIBUTE13,
		GLOBAL_ATTRIBUTE14,
		GLOBAL_ATTRIBUTE15,
		GLOBAL_ATTRIBUTE16,
		GLOBAL_ATTRIBUTE17,
		GLOBAL_ATTRIBUTE18,
		GLOBAL_ATTRIBUTE19,
		GLOBAL_ATTRIBUTE20,
		GLOBAL_ATTRIBUTE_CATEGORY,
		DEPRN_OVERRIDE_FLAG,
		TRACK_MEMBER_FLAG,
		ADJUSTMENT_LINE_ID,
		SOURCE_LINE_ID,
		SOURCE_DEST_CODE,
		INSERTION_ORDER,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.fa_adjustments;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_adjustments';
	
Commit;