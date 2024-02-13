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

DROP TABLE if exists bec_ods.OKC_K_LINES_B;

CREATE TABLE IF NOT EXISTS bec_ods.OKC_K_LINES_B
(
	ID VARCHAR(50) ENCODE LZO 
	,LINE_NUMBER VARCHAR(150) ENCODE LZO
	,CHR_ID NUMERIC(15,0) ENCODE az64 
	,CLE_ID VARCHAR(50) ENCODE LZO 
	,CLE_ID_RENEWED NUMERIC(15,0) ENCODE az64 
	,DNZ_CHR_ID NUMERIC(15,0) ENCODE az64 
	,DISPLAY_SEQUENCE NUMERIC(7,0) ENCODE az64 
	,STS_CODE VARCHAR(30) ENCODE LZO 
	,TRN_CODE VARCHAR(30) ENCODE LZO 
	,LSE_ID NUMERIC(15,0) ENCODE az64 
	,EXCEPTION_YN VARCHAR(3) ENCODE LZO 
	,OBJECT_VERSION_NUMBER NUMERIC(9,0) ENCODE az64 
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,HIDDEN_IND VARCHAR(3) ENCODE LZO 
	,PRICE_NEGOTIATED NUMERIC(28,10) ENCODE az64 
	,PRICE_LEVEL_IND VARCHAR(3) ENCODE LZO 
	,PRICE_UNIT NUMERIC(28,10) ENCODE az64 
	,PRICE_UNIT_PERCENT NUMERIC(6,2) ENCODE az64 
	,INVOICE_LINE_LEVEL_IND VARCHAR(3) ENCODE LZO 
	,DPAS_RATING VARCHAR(24) ENCODE LZO 
	,TEMPLATE_USED VARCHAR(150) ENCODE LZO 
	,PRICE_TYPE VARCHAR(30) ENCODE LZO 
	,CURRENCY_CODE VARCHAR(15) ENCODE LZO 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 
	,DATE_TERMINATED TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,START_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,END_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,ATTRIBUTE_CATEGORY VARCHAR(90) ENCODE LZO 
	,ATTRIBUTE1 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE2 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE3 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE4 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE5 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE6 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE7 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE8 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE9 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE10 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE11 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE12 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE13 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE14 VARCHAR(450) ENCODE LZO 
	,ATTRIBUTE15 VARCHAR(450) ENCODE LZO 
	,SECURITY_GROUP_ID NUMERIC(15,0) ENCODE az64 
	,CLE_ID_RENEWED_TO NUMERIC(15,0) ENCODE az64 
	,PRICE_NEGOTIATED_RENEWED NUMERIC(28,10) ENCODE az64 
	,CURRENCY_CODE_RENEWED VARCHAR(15) ENCODE LZO 
	,UPG_ORIG_SYSTEM_REF VARCHAR(60) ENCODE LZO
	,UPG_ORIG_SYSTEM_REF_ID NUMERIC(15,0) ENCODE az64 
	,DATE_RENEWED TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,ORIG_SYSTEM_SOURCE_CODE VARCHAR(30) ENCODE LZO 
	,ORIG_SYSTEM_ID1 VARCHAR(50) ENCODE LZO 
	,ORIG_SYSTEM_REFERENCE1 VARCHAR(30) ENCODE LZO 
	,PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,REQUEST_ID NUMERIC(15,0) ENCODE az64 
	,PRICE_LIST_ID NUMERIC(15,0) ENCODE az64 
	,PRICE_LIST_LINE_ID NUMERIC(15,0) ENCODE az64 
	,LINE_LIST_PRICE NUMERIC(28,10) ENCODE az64 
	,ITEM_TO_PRICE_YN VARCHAR(3) ENCODE LZO 
	,PRICING_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,PRICE_BASIS_YN VARCHAR(3) ENCODE LZO 
	,CONFIG_HEADER_ID NUMERIC(15,0) ENCODE az64 
	,CONFIG_REVISION_NUMBER NUMERIC(15,0) ENCODE az64 
	,CONFIG_COMPLETE_YN VARCHAR(3) ENCODE LZO 
	,CONFIG_VALID_YN VARCHAR(3) ENCODE LZO 
	,CONFIG_TOP_MODEL_LINE_ID NUMERIC(15,0) ENCODE az64 
	,CONFIG_ITEM_TYPE VARCHAR(30) ENCODE LZO 
	,CONFIG_ITEM_ID NUMERIC(15,0) ENCODE az64 
	,SERVICE_ITEM_YN VARCHAR(3) ENCODE LZO 
	,PH_PRICING_TYPE VARCHAR(30) ENCODE LZO 
	,PH_PRICE_BREAK_BASIS VARCHAR(30) ENCODE LZO 
	,PH_MIN_QTY NUMERIC(28,10) ENCODE az64 
	,PH_MIN_AMT NUMERIC(28,10) ENCODE az64 
	,PH_QP_REFERENCE_ID NUMERIC(15,0) ENCODE az64 
	,PH_VALUE NUMERIC(28,10) ENCODE az64 
	,PH_ENFORCE_PRICE_LIST_YN VARCHAR(1) ENCODE LZO 
	,PH_ADJUSTMENT NUMERIC(28,10) ENCODE az64 
	,PH_INTEGRATED_WITH_QP VARCHAR(1) ENCODE LZO 
	,CUST_ACCT_ID NUMERIC(15,0) ENCODE az64 
	,BILL_TO_SITE_USE_ID NUMERIC(15,0) ENCODE az64 
	,INV_RULE_ID NUMERIC(15,0) ENCODE az64 
	,LINE_RENEWAL_TYPE_CODE VARCHAR(30) ENCODE LZO 
	,SHIP_TO_SITE_USE_ID NUMERIC(15,0) ENCODE az64 
	,PAYMENT_TERM_ID NUMERIC(15,0) ENCODE az64 
	,DATE_CANCELLED TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,TERM_CANCEL_SOURCE VARCHAR(30) ENCODE LZO 
	,PAYMENT_INSTRUCTION_TYPE VARCHAR(3) ENCODE LZO 
	,ANNUALIZED_FACTOR NUMERIC(28,10) ENCODE az64 
	,CANCELLED_AMOUNT NUMERIC(28,10) ENCODE az64 
	,GROUP_ID NUMERIC(15,0) ENCODE az64 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

COMMIT;

INSERT INTO bec_ods.OKC_K_LINES_B (
	ID,
	LINE_NUMBER,
	CHR_ID,
	CLE_ID,
	CLE_ID_RENEWED,
	DNZ_CHR_ID,
	DISPLAY_SEQUENCE,
	STS_CODE,
	TRN_CODE,
	LSE_ID,
	EXCEPTION_YN,
	OBJECT_VERSION_NUMBER,
	CREATED_BY,
	CREATION_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	HIDDEN_IND,
	PRICE_NEGOTIATED,
	PRICE_LEVEL_IND,
	PRICE_UNIT,
	PRICE_UNIT_PERCENT,
	INVOICE_LINE_LEVEL_IND,
	DPAS_RATING,
	TEMPLATE_USED,
	PRICE_TYPE,
	CURRENCY_CODE,
	LAST_UPDATE_LOGIN,
	DATE_TERMINATED,
	START_DATE,
	END_DATE,
	ATTRIBUTE_CATEGORY,
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
	SECURITY_GROUP_ID,
	CLE_ID_RENEWED_TO,
	PRICE_NEGOTIATED_RENEWED,
	CURRENCY_CODE_RENEWED,
	UPG_ORIG_SYSTEM_REF,
	UPG_ORIG_SYSTEM_REF_ID,
	DATE_RENEWED,
	ORIG_SYSTEM_SOURCE_CODE,
	ORIG_SYSTEM_ID1,
	ORIG_SYSTEM_REFERENCE1,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	REQUEST_ID,
	PRICE_LIST_ID,
	PRICE_LIST_LINE_ID,
	LINE_LIST_PRICE,
	ITEM_TO_PRICE_YN,
	PRICING_DATE,
	PRICE_BASIS_YN,
	CONFIG_HEADER_ID,
	CONFIG_REVISION_NUMBER,
	CONFIG_COMPLETE_YN,
	CONFIG_VALID_YN,
	CONFIG_TOP_MODEL_LINE_ID,
	CONFIG_ITEM_TYPE,
	CONFIG_ITEM_ID,
	SERVICE_ITEM_YN,
	PH_PRICING_TYPE,
	PH_PRICE_BREAK_BASIS,
	PH_MIN_QTY,
	PH_MIN_AMT,
	PH_QP_REFERENCE_ID,
	PH_VALUE,
	PH_ENFORCE_PRICE_LIST_YN,
	PH_ADJUSTMENT,
	PH_INTEGRATED_WITH_QP,
	CUST_ACCT_ID,
	BILL_TO_SITE_USE_ID,
	INV_RULE_ID,
	LINE_RENEWAL_TYPE_CODE,
	SHIP_TO_SITE_USE_ID,
	PAYMENT_TERM_ID,
	DATE_CANCELLED,
	TERM_CANCEL_SOURCE,
	PAYMENT_INSTRUCTION_TYPE,
	ANNUALIZED_FACTOR,
	CANCELLED_AMOUNT,
	GROUP_ID,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		ID,
		LINE_NUMBER,
		CHR_ID,
		CLE_ID,
		CLE_ID_RENEWED,
		DNZ_CHR_ID,
		DISPLAY_SEQUENCE,
		STS_CODE,
		TRN_CODE,
		LSE_ID,
		EXCEPTION_YN,
		OBJECT_VERSION_NUMBER,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		HIDDEN_IND,
		PRICE_NEGOTIATED,
		PRICE_LEVEL_IND,
		PRICE_UNIT,
		PRICE_UNIT_PERCENT,
		INVOICE_LINE_LEVEL_IND,
		DPAS_RATING,
		TEMPLATE_USED,
		PRICE_TYPE,
		CURRENCY_CODE,
		LAST_UPDATE_LOGIN,
		DATE_TERMINATED,
		START_DATE,
		END_DATE,
		ATTRIBUTE_CATEGORY,
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
		SECURITY_GROUP_ID,
		CLE_ID_RENEWED_TO,
		PRICE_NEGOTIATED_RENEWED,
		CURRENCY_CODE_RENEWED,
		UPG_ORIG_SYSTEM_REF,
		UPG_ORIG_SYSTEM_REF_ID,
		DATE_RENEWED,
		ORIG_SYSTEM_SOURCE_CODE,
		ORIG_SYSTEM_ID1,
		ORIG_SYSTEM_REFERENCE1,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
		REQUEST_ID,
		PRICE_LIST_ID,
		PRICE_LIST_LINE_ID,
		LINE_LIST_PRICE,
		ITEM_TO_PRICE_YN,
		PRICING_DATE,
		PRICE_BASIS_YN,
		CONFIG_HEADER_ID,
		CONFIG_REVISION_NUMBER,
		CONFIG_COMPLETE_YN,
		CONFIG_VALID_YN,
		CONFIG_TOP_MODEL_LINE_ID,
		CONFIG_ITEM_TYPE,
		CONFIG_ITEM_ID,
		SERVICE_ITEM_YN,
		PH_PRICING_TYPE,
		PH_PRICE_BREAK_BASIS,
		PH_MIN_QTY,
		PH_MIN_AMT,
		PH_QP_REFERENCE_ID,
		PH_VALUE,
		PH_ENFORCE_PRICE_LIST_YN,
		PH_ADJUSTMENT,
		PH_INTEGRATED_WITH_QP,
		CUST_ACCT_ID,
		BILL_TO_SITE_USE_ID,
		INV_RULE_ID,
		LINE_RENEWAL_TYPE_CODE,
		SHIP_TO_SITE_USE_ID,
		PAYMENT_TERM_ID,
		DATE_CANCELLED,
		TERM_CANCEL_SOURCE,
		PAYMENT_INSTRUCTION_TYPE,
		ANNUALIZED_FACTOR,
		CANCELLED_AMOUNT,
		GROUP_ID,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.OKC_K_LINES_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'okc_k_lines_b';
	
commit;