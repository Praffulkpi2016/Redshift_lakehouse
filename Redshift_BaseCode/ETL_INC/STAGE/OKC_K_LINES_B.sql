/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.OKC_K_LINES_B;

COMMIT;

insert
	into
	bec_ods_stg.OKC_K_LINES_B
(	ID,
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
	kca_seq_id,
	kca_seq_date
)
(
	select
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
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.OKC_K_LINES_B
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(ID, 'NA'),
		nvl(CHR_ID, 'NA'),
		nvl(CLE_ID, 'NA'),
		nvl(LINE_NUMBER, 'NA'),
		kca_seq_id) in 
(
		select
			nvl(ID,'NA') as ID,
			nvl(CHR_ID, 'NA') as CHR_ID,
			nvl(CLE_ID, 'NA') as CLE_ID,
			nvl(LINE_NUMBER, 'NA') as LINE_NUMBER,
			max(kca_seq_id) as kca_seq_id
		from
			bec_raw_dl_ext.OKC_K_LINES_B
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(ID, 'NA'),
			nvl(CHR_ID, 'NA'),
			nvl(CLE_ID, 'NA'),
			nvl(LINE_NUMBER, 'NA'))
		and 
		kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'okc_k_lines_b')
);
end;