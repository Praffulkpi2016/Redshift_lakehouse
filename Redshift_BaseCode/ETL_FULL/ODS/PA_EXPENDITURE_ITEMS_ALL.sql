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

DROP TABLE if exists bec_ods.pa_expenditure_items_all;

CREATE TABLE IF NOT EXISTS bec_ods.pa_expenditure_items_all
(
	EXPENDITURE_ITEM_ID NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,EXPENDITURE_ID NUMERIC(15,0) ENCODE az64 
	,TASK_ID NUMERIC(15,0) ENCODE az64
	,EXPENDITURE_ITEM_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,EXPENDITURE_TYPE VARCHAR(30) ENCODE lzo  	
	,COST_DISTRIBUTED_FLAG VARCHAR(1) ENCODE lzo 	
	,REVENUE_DISTRIBUTED_FLAG VARCHAR(1) ENCODE lzo 	
	,BILLABLE_FLAG VARCHAR(1) ENCODE lzo 	
	,BILL_HOLD_FLAG VARCHAR(1) ENCODE lzo
	,QUANTITY NUMERIC(28,10) ENCODE az64 
	,NON_LABOR_RESOURCE VARCHAR(20) ENCODE lzo
	,ORGANIZATION_ID NUMERIC(15,0) ENCODE az64 
	,OVERRIDE_TO_ORGANIZATION_ID NUMERIC(15,0) ENCODE az64  
	,RAW_COST NUMERIC(28,10) ENCODE az64 
	,RAW_COST_RATE NUMERIC(28,10) ENCODE az64 
	,BURDEN_COST NUMERIC(28,10) ENCODE az64 
	,BURDEN_COST_RATE NUMERIC(28,10) ENCODE az64 
	,COST_DIST_REJECTION_CODE VARCHAR(30) ENCODE lzo
	,LABOR_COST_MULTIPLIER_NAME VARCHAR(20) ENCODE lzo
	,RAW_REVENUE NUMERIC(22,5) ENCODE az64	
	,BILL_RATE NUMERIC(22,5) ENCODE az64	
	,ACCRUED_REVENUE NUMERIC(22,5) ENCODE az64	
	,ACCRUAL_RATE NUMERIC(22,5) ENCODE az64	
	,ADJUSTED_REVENUE NUMERIC(22,5) ENCODE az64	
	,ADJUSTED_RATE NUMERIC(22,5) ENCODE az64	
	,BILL_AMOUNT NUMERIC(22,5) ENCODE az64	
	,FORECAST_REVENUE NUMERIC(22,5) ENCODE az64	
	,BILL_RATE_MULTIPLIER NUMERIC(22,5) ENCODE az64
	,REV_DIST_REJECTION_CODE VARCHAR(30) ENCODE lzo 
	,EVENT_NUM NUMERIC(15,0) ENCODE az64 
	,EVENT_TASK_ID NUMERIC(15,0) ENCODE az64 
	,BILL_JOB_ID NUMERIC(15,0) ENCODE az64  
	,BILL_JOB_BILLING_TITLE VARCHAR(80) ENCODE lzo  
	,BILL_EMPLOYEE_BILLING_TITLE VARCHAR(80) ENCODE lzo 		
	,ADJUSTED_EXPENDITURE_ITEM_ID NUMERIC(15,0) ENCODE az64  
	,NET_ZERO_ADJUSTMENT_FLAG VARCHAR(1) ENCODE lzo 			
	,TRANSFERRED_FROM_EXP_ITEM_ID NUMERIC(15,0) ENCODE az64  
	,CONVERTED_FLAG VARCHAR(1) ENCODE lzo 	
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 
	,REQUEST_ID NUMERIC(15,0) ENCODE az64 	
	,PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_ID NUMERIC(15,0) ENCODE az64 	
	,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,ATTRIBUTE_CATEGORY VARCHAR(30) ENCODE lzo 
	,ATTRIBUTE1 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE2 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE3 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE4 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE5 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE6 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE7 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE8 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE9 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE10 VARCHAR(150) ENCODE lzo 
	,COST_IND_COMPILED_SET_ID NUMERIC(15,0) ENCODE az64
	,REV_IND_COMPILED_SET_ID NUMERIC(15,0) ENCODE az64
	,INV_IND_COMPILED_SET_ID NUMERIC(15,0) ENCODE az64
	,COST_BURDEN_DISTRIBUTED_FLAG VARCHAR(1) ENCODE lzo
	,IND_COST_DIST_REJECTION_CODE VARCHAR(30) ENCODE lzo
	,ORIG_TRANSACTION_REFERENCE VARCHAR(50) ENCODE lzo
	,TRANSACTION_SOURCE VARCHAR(30) ENCODE lzo
	,PROJECT_ID NUMERIC(15,0) ENCODE az64
	,SOURCE_EXPENDITURE_ITEM_ID NUMERIC(15,0) ENCODE az64
	,JOB_ID NUMERIC(15,0) ENCODE az64
	,ORG_ID NUMERIC(15,0) ENCODE az64
	,SYSTEM_LINKAGE_FUNCTION VARCHAR(3) ENCODE lzo
	,BURDEN_SUM_DEST_RUN_ID NUMERIC(15,0) ENCODE az64
	,RECEIPT_CURRENCY_AMOUNT NUMERIC(28,10) ENCODE az64
	,RECEIPT_CURRENCY_CODE VARCHAR(15) ENCODE lzo
	,RECEIPT_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64
	,DENOM_CURRENCY_CODE VARCHAR(15) ENCODE lzo
	,DENOM_RAW_COST NUMERIC(28,10) ENCODE az64
	,DENOM_BURDENED_COST NUMERIC(28,10) ENCODE az64
	,ACCT_CURRENCY_CODE VARCHAR(15) ENCODE lzo	
	,ACCT_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,ACCT_RATE_TYPE VARCHAR(30) ENCODE lzo
	,ACCT_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64
	,ACCT_RAW_COST NUMERIC(28,10) ENCODE az64
	,ACCT_BURDENED_COST NUMERIC(28,10) ENCODE az64
	,ACCT_EXCHANGE_ROUNDING_LIMIT NUMERIC(28,10) ENCODE az64
	,PROJECT_CURRENCY_CODE VARCHAR(15) ENCODE lzo	
	,PROJECT_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,PROJECT_RATE_TYPE VARCHAR(30) ENCODE lzo
	,PROJECT_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	
	,DENORM_ID NUMERIC(15,0) ENCODE az64 
	,CC_CROSS_CHARGE_CODE VARCHAR(1) ENCODE lzo
	,CC_PRVDR_ORGANIZATION_ID NUMERIC(15,0) ENCODE az64	
	,CC_RECVR_ORGANIZATION_ID NUMERIC(15,0) ENCODE az64
	,CC_REJECTION_CODE VARCHAR(30) ENCODE lzo 
	,DENOM_TP_CURRENCY_CODE VARCHAR(15) ENCODE lzo 	
	,DENOM_TRANSFER_PRICE NUMERIC(28,10) ENCODE az64
	,ACCT_TP_RATE_TYPE VARCHAR(30) ENCODE lzo		
	,ACCT_TP_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,ACCT_TP_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64
	,ACCT_TRANSFER_PRICE NUMERIC(28,10) ENCODE az64
	,PROJACCT_TRANSFER_PRICE NUMERIC(28,10) ENCODE az64
	,CC_MARKUP_BASE_CODE VARCHAR(1) ENCODE lzo
	,TP_BASE_AMOUNT NUMERIC(28,10) ENCODE az64
	,CC_CROSS_CHARGE_TYPE VARCHAR(2) ENCODE lzo
	,RECVR_ORG_ID NUMERIC(15,0) ENCODE az64	
	,CC_BL_DISTRIBUTED_CODE VARCHAR(1) ENCODE lzo
	,CC_IC_PROCESSED_CODE VARCHAR(1) ENCODE lzo	
	,TP_IND_COMPILED_SET_ID NUMERIC(15,0) ENCODE az64	
	,TP_BILL_RATE NUMERIC(28,10) ENCODE az64
	,TP_BILL_MARKUP_PERCENTAGE NUMERIC(28,10) ENCODE az64
	,TP_SCHEDULE_LINE_PERCENTAGE NUMERIC(28,10) ENCODE az64
	,TP_RULE_PERCENTAGE NUMERIC(28,10) ENCODE az64	
	,CC_PRVDR_COST_RECLASS_CODE VARCHAR(1) ENCODE lzo
	,CRL_ASSET_CREATION_STATUS_CODE VARCHAR(1) ENCODE lzo	
	,CRL_ASSET_CREATION_REJ_CODE VARCHAR(30) ENCODE lzo
	,COST_JOB_ID NUMERIC(15,0) ENCODE az64
	,TP_JOB_ID NUMERIC(15,0) ENCODE az64
	,PROV_PROJ_BILL_JOB_ID NUMERIC(15,0) ENCODE az64	
	,COST_DIST_WARNING_CODE VARCHAR(30) ENCODE lzo		
	,PROJECT_TP_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJECT_TP_RATE_TYPE VARCHAR(30) ENCODE lzo	
	,PROJECT_TP_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64		
	,PROJFUNC_TP_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJFUNC_TP_RATE_TYPE VARCHAR(30) ENCODE lzo	
	,PROJFUNC_TP_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64
	,PROJFUNC_TRANSFER_PRICE NUMERIC(28,10) ENCODE az64		
	,BILL_TRANS_FORECAST_CURR_CODE VARCHAR(15) ENCODE lzo	
	,BILL_TRANS_FORECAST_REVENUE NUMERIC(28,10) ENCODE az64		
	,PROJFUNC_REV_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJFUNC_REV_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	
	,PROJFUNC_COST_RATE_TYPE VARCHAR(30) ENCODE lzo		
	,PROJFUNC_COST_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJFUNC_COST_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	 	
	,PROJECT_RAW_COST NUMERIC(28,10) ENCODE az64	 	
	,PROJECT_BURDENED_COST NUMERIC(28,10) ENCODE az64	
	,ASSIGNMENT_ID NUMERIC(15,0) ENCODE az64	
	,WORK_TYPE_ID NUMERIC(15,0) ENCODE az64		
	,PROJFUNC_RAW_REVENUE NUMERIC(28,10) ENCODE az64	 	
	,PROJECT_BILL_AMOUNT NUMERIC(28,10) ENCODE az64	
	,PROJFUNC_CURRENCY_CODE VARCHAR(15) ENCODE lzo 
	,PROJECT_RAW_REVENUE NUMERIC(28,10) ENCODE az64	 	
	,PROJECT_TRANSFER_PRICE NUMERIC(28,10) ENCODE az64	
	,TP_AMT_TYPE_CODE VARCHAR(30) ENCODE lzo 
	,BILL_TRANS_CURRENCY_CODE VARCHAR(15) ENCODE lzo 	
	,BILL_TRANS_RAW_REVENUE NUMERIC(28,10) ENCODE az64	
	,BILL_TRANS_BILL_AMOUNT NUMERIC(28,10) ENCODE az64	
	,BILL_TRANS_ADJUSTED_REVENUE NUMERIC(28,10) ENCODE az64
	,REVPROC_CURRENCY_CODE VARCHAR(15) ENCODE lzo 
	,REVPROC_RATE_TYPE VARCHAR(30) ENCODE lzo 	
	,REVPROC_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,REVPROC_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	
	,INVPROC_CURRENCY_CODE VARCHAR(15) ENCODE lzo 
	,INVPROC_RATE_TYPE VARCHAR(30) ENCODE lzo 
	,INVPROC_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,DISCOUNT_PERCENTAGE NUMERIC(28,10) ENCODE az64		
	,LABOR_MULTIPLIER NUMERIC(28,10) ENCODE az64	
	,AMOUNT_CALCULATION_CODE VARCHAR(15) ENCODE lzo 	
	,BILL_MARKUP_PERCENTAGE NUMERIC(28,10) ENCODE az64		
	,RATE_SOURCE_ID NUMERIC(15,0) ENCODE az64 	
	,INVPROC_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64 
	,INV_GEN_REJECTION_CODE VARCHAR(30) ENCODE lzo  	
	,PROJFUNC_BILL_AMOUNT NUMERIC(28,10) ENCODE az64
	,PROJECT_REV_RATE_TYPE VARCHAR(30) ENCODE lzo 
	,PROJECT_REV_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJECT_REV_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	
	,PROJFUNC_REV_RATE_TYPE VARCHAR(30) ENCODE lzo 
	,PROJFUNC_INV_RATE_TYPE VARCHAR(30) ENCODE lzo 
	,PROJFUNC_INV_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJFUNC_INV_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	
	,PROJECT_INV_RATE_TYPE VARCHAR(30) ENCODE lzo 
	,PROJECT_INV_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJECT_INV_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	
	,PROJFUNC_FCST_RATE_TYPE VARCHAR(30) ENCODE lzo 
	,PROJFUNC_FCST_RATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,PROJFUNC_FCST_EXCHANGE_RATE NUMERIC(28,10) ENCODE az64	 
	,PRVDR_ACCRUAL_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,RECVR_ACCRUAL_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,RATE_DISC_REASON_CODE VARCHAR(30) ENCODE lzo	
	,POSTED_DENOM_BURDENED_COST NUMERIC(28,10) ENCODE az64		
	,POSTED_PROJECT_BURDENED_COST NUMERIC(28,10) ENCODE az64		
	,POSTED_PROJFUNC_BURDENED_COST NUMERIC(28,10) ENCODE az64		
	,POSTED_ACCT_BURDENED_COST NUMERIC(28,10) ENCODE az64	
	,ADJUSTMENT_TYPE VARCHAR(150) ENCODE lzo		
	,CAPITAL_EVENT_ID NUMERIC(15,0) ENCODE az64 		
	,PO_LINE_ID NUMERIC(28,10) ENCODE az64	 	
	,PO_PRICE_TYPE VARCHAR(30) ENCODE lzo			
	,WIP_RESOURCE_ID NUMERIC(15,0) ENCODE az64		
	,INVENTORY_ITEM_ID NUMERIC(15,0) ENCODE az64
	,UNIT_OF_MEASURE VARCHAR(30) ENCODE lzo	
	,SRC_SYSTEM_LINKAGE_FUNCTION VARCHAR(3) ENCODE lzo	
	,DOCUMENT_HEADER_ID NUMERIC(15,0) ENCODE az64	
	,DOCUMENT_DISTRIBUTION_ID NUMERIC(15,0) ENCODE az64	
	,DOCUMENT_LINE_NUMBER NUMERIC(15,0) ENCODE az64	
	,DOCUMENT_PAYMENT_ID NUMERIC(15,0) ENCODE az64	
	,VENDOR_ID NUMERIC(15,0) ENCODE az64	
	,DOCUMENT_TYPE VARCHAR(30) ENCODE lzo	
	,DOCUMENT_DISTRIBUTION_TYPE VARCHAR(30) ENCODE lzo	
	,HISTORICAL_FLAG VARCHAR(1) ENCODE lzo	
	,LOCATION_ID NUMERIC(15,0) ENCODE az64	
	,PAY_ELEMENT_TYPE_ID NUMERIC(15,0) ENCODE az64
	,COSTING_METHOD VARCHAR(150) ENCODE lzo		
	,RATE_SOURCE_CODE VARCHAR(30) ENCODE lzo	
	,PAYROLL_ACCRUAL_FLAG VARCHAR(1) ENCODE lzo	
	,RBC_ELEMENT_TYPE_ID NUMERIC(9,0) ENCODE az64	
	,INTERFACE_RUN_ID NUMERIC(15,0) ENCODE az64	
	,CBS_ELEMENT_ID NUMERIC(15,0) ENCODE az64	
	,ADD_INV_GROUP VARCHAR(40) ENCODE lzo	
	,BILL_GROUP VARCHAR(40) ENCODE lzo	
	,LEGAL_ENTITY_ID NUMERIC(15,0) ENCODE az64	
	,RECEIVER_LEGAL_ENTITY_ID NUMERIC(15,0) ENCODE az64	
	,REVENUE_HOLD_FLAG VARCHAR(1) ENCODE lzo	
	,FCB_PROCESS_FLAG VARCHAR(1) ENCODE lzo	 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.pa_expenditure_items_all (
	EXPENDITURE_ITEM_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	EXPENDITURE_ID,
	TASK_ID,
	EXPENDITURE_ITEM_DATE,
	EXPENDITURE_TYPE,
	COST_DISTRIBUTED_FLAG,
	REVENUE_DISTRIBUTED_FLAG,
	BILLABLE_FLAG,
	BILL_HOLD_FLAG,
	QUANTITY,
	NON_LABOR_RESOURCE,
	ORGANIZATION_ID,
	OVERRIDE_TO_ORGANIZATION_ID,
	RAW_COST,
	RAW_COST_RATE,
	BURDEN_COST,
	BURDEN_COST_RATE,
	COST_DIST_REJECTION_CODE,
	LABOR_COST_MULTIPLIER_NAME,
	RAW_REVENUE,
	BILL_RATE,
	ACCRUED_REVENUE,
	ACCRUAL_RATE,
	ADJUSTED_REVENUE,
	ADJUSTED_RATE,
	BILL_AMOUNT,
	FORECAST_REVENUE,
	BILL_RATE_MULTIPLIER,
	REV_DIST_REJECTION_CODE,
	EVENT_NUM,
	EVENT_TASK_ID,
	BILL_JOB_ID,
	BILL_JOB_BILLING_TITLE,
	BILL_EMPLOYEE_BILLING_TITLE,
	ADJUSTED_EXPENDITURE_ITEM_ID,
	NET_ZERO_ADJUSTMENT_FLAG,
	TRANSFERRED_FROM_EXP_ITEM_ID,
	CONVERTED_FLAG,
	LAST_UPDATE_LOGIN,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
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
	COST_IND_COMPILED_SET_ID,
	REV_IND_COMPILED_SET_ID,
	INV_IND_COMPILED_SET_ID,
	COST_BURDEN_DISTRIBUTED_FLAG,
	IND_COST_DIST_REJECTION_CODE,
	ORIG_TRANSACTION_REFERENCE,
	TRANSACTION_SOURCE,
	PROJECT_ID,
	SOURCE_EXPENDITURE_ITEM_ID,
	JOB_ID,
	ORG_ID,
	SYSTEM_LINKAGE_FUNCTION,
	BURDEN_SUM_DEST_RUN_ID,
	RECEIPT_CURRENCY_AMOUNT,
	RECEIPT_CURRENCY_CODE,
	RECEIPT_EXCHANGE_RATE,
	DENOM_CURRENCY_CODE,
	DENOM_RAW_COST,
	DENOM_BURDENED_COST,
	ACCT_CURRENCY_CODE,
	ACCT_RATE_DATE,
	ACCT_RATE_TYPE,
	ACCT_EXCHANGE_RATE,
	ACCT_RAW_COST,
	ACCT_BURDENED_COST,
	ACCT_EXCHANGE_ROUNDING_LIMIT,
	PROJECT_CURRENCY_CODE,
	PROJECT_RATE_DATE,
	PROJECT_RATE_TYPE,
	PROJECT_EXCHANGE_RATE,
	DENORM_ID,
	CC_CROSS_CHARGE_CODE,
	CC_PRVDR_ORGANIZATION_ID,
	CC_RECVR_ORGANIZATION_ID,
	CC_REJECTION_CODE,
	DENOM_TP_CURRENCY_CODE,
	DENOM_TRANSFER_PRICE,
	ACCT_TP_RATE_TYPE,
	ACCT_TP_RATE_DATE,
	ACCT_TP_EXCHANGE_RATE,
	ACCT_TRANSFER_PRICE,
	PROJACCT_TRANSFER_PRICE,
	CC_MARKUP_BASE_CODE,
	TP_BASE_AMOUNT,
	CC_CROSS_CHARGE_TYPE,
	RECVR_ORG_ID,
	CC_BL_DISTRIBUTED_CODE,
	CC_IC_PROCESSED_CODE,
	TP_IND_COMPILED_SET_ID,
	TP_BILL_RATE,
	TP_BILL_MARKUP_PERCENTAGE,
	TP_SCHEDULE_LINE_PERCENTAGE,
	TP_RULE_PERCENTAGE,
	CC_PRVDR_COST_RECLASS_CODE,
	CRL_ASSET_CREATION_STATUS_CODE,
	CRL_ASSET_CREATION_REJ_CODE,
	COST_JOB_ID,
	TP_JOB_ID,
	PROV_PROJ_BILL_JOB_ID,
	COST_DIST_WARNING_CODE,
	PROJECT_TP_RATE_DATE,
	PROJECT_TP_RATE_TYPE,
	PROJECT_TP_EXCHANGE_RATE,
	PROJFUNC_TP_RATE_DATE,
	PROJFUNC_TP_RATE_TYPE,
	PROJFUNC_TP_EXCHANGE_RATE,
	PROJFUNC_TRANSFER_PRICE,
	BILL_TRANS_FORECAST_CURR_CODE,
	BILL_TRANS_FORECAST_REVENUE,
	PROJFUNC_REV_RATE_DATE,
	PROJFUNC_REV_EXCHANGE_RATE,
	PROJFUNC_COST_RATE_TYPE,
	PROJFUNC_COST_RATE_DATE,
	PROJFUNC_COST_EXCHANGE_RATE,
	PROJECT_RAW_COST,
	PROJECT_BURDENED_COST,
	ASSIGNMENT_ID,
	WORK_TYPE_ID,
	PROJFUNC_RAW_REVENUE,
	PROJECT_BILL_AMOUNT,
	PROJFUNC_CURRENCY_CODE,
	PROJECT_RAW_REVENUE,
	PROJECT_TRANSFER_PRICE,
	TP_AMT_TYPE_CODE,
	BILL_TRANS_CURRENCY_CODE,
	BILL_TRANS_RAW_REVENUE,
	BILL_TRANS_BILL_AMOUNT,
	BILL_TRANS_ADJUSTED_REVENUE,
	REVPROC_CURRENCY_CODE,
	REVPROC_RATE_TYPE,
	REVPROC_RATE_DATE,
	REVPROC_EXCHANGE_RATE,
	INVPROC_CURRENCY_CODE,
	INVPROC_RATE_TYPE,
	INVPROC_RATE_DATE,
	DISCOUNT_PERCENTAGE,
	LABOR_MULTIPLIER,
	AMOUNT_CALCULATION_CODE,
	BILL_MARKUP_PERCENTAGE,
	RATE_SOURCE_ID,
	INVPROC_EXCHANGE_RATE,
	INV_GEN_REJECTION_CODE,
	PROJFUNC_BILL_AMOUNT,
	PROJECT_REV_RATE_TYPE,
	PROJECT_REV_RATE_DATE,
	PROJECT_REV_EXCHANGE_RATE,
	PROJFUNC_REV_RATE_TYPE,
	PROJFUNC_INV_RATE_TYPE,
	PROJFUNC_INV_RATE_DATE,
	PROJFUNC_INV_EXCHANGE_RATE,
	PROJECT_INV_RATE_TYPE,
	PROJECT_INV_RATE_DATE,
	PROJECT_INV_EXCHANGE_RATE,
	PROJFUNC_FCST_RATE_TYPE,
	PROJFUNC_FCST_RATE_DATE,
	PROJFUNC_FCST_EXCHANGE_RATE,
	PRVDR_ACCRUAL_DATE,
	RECVR_ACCRUAL_DATE,
	RATE_DISC_REASON_CODE,
	POSTED_DENOM_BURDENED_COST,
	POSTED_PROJECT_BURDENED_COST,
	POSTED_PROJFUNC_BURDENED_COST,
	POSTED_ACCT_BURDENED_COST,
	ADJUSTMENT_TYPE,
	CAPITAL_EVENT_ID,
	PO_LINE_ID,
	PO_PRICE_TYPE,
	WIP_RESOURCE_ID,
	INVENTORY_ITEM_ID,
	UNIT_OF_MEASURE,
	SRC_SYSTEM_LINKAGE_FUNCTION,
	DOCUMENT_HEADER_ID,
	DOCUMENT_DISTRIBUTION_ID,
	DOCUMENT_LINE_NUMBER,
	DOCUMENT_PAYMENT_ID,
	VENDOR_ID,
	DOCUMENT_TYPE,
	DOCUMENT_DISTRIBUTION_TYPE,
	HISTORICAL_FLAG,
	LOCATION_ID,
	PAY_ELEMENT_TYPE_ID,
	COSTING_METHOD,
	RATE_SOURCE_CODE,
	PAYROLL_ACCRUAL_FLAG,
	RBC_ELEMENT_TYPE_ID,
	INTERFACE_RUN_ID,
	CBS_ELEMENT_ID,
	ADD_INV_GROUP,
	BILL_GROUP,
	LEGAL_ENTITY_ID,
	RECEIVER_LEGAL_ENTITY_ID,
	REVENUE_HOLD_FLAG,
	FCB_PROCESS_FLAG,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
		EXPENDITURE_ITEM_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		EXPENDITURE_ID,
		TASK_ID,
		EXPENDITURE_ITEM_DATE,
		EXPENDITURE_TYPE,
		COST_DISTRIBUTED_FLAG,
		REVENUE_DISTRIBUTED_FLAG,
		BILLABLE_FLAG,
		BILL_HOLD_FLAG,
		QUANTITY,
		NON_LABOR_RESOURCE,
		ORGANIZATION_ID,
		OVERRIDE_TO_ORGANIZATION_ID,
		RAW_COST,
		RAW_COST_RATE,
		BURDEN_COST,
		BURDEN_COST_RATE,
		COST_DIST_REJECTION_CODE,
		LABOR_COST_MULTIPLIER_NAME,
		RAW_REVENUE,
		BILL_RATE,
		ACCRUED_REVENUE,
		ACCRUAL_RATE,
		ADJUSTED_REVENUE,
		ADJUSTED_RATE,
		BILL_AMOUNT,
		FORECAST_REVENUE,
		BILL_RATE_MULTIPLIER,
		REV_DIST_REJECTION_CODE,
		EVENT_NUM,
		EVENT_TASK_ID,
		BILL_JOB_ID,
		BILL_JOB_BILLING_TITLE,
		BILL_EMPLOYEE_BILLING_TITLE,
		ADJUSTED_EXPENDITURE_ITEM_ID,
		NET_ZERO_ADJUSTMENT_FLAG,
		TRANSFERRED_FROM_EXP_ITEM_ID,
		CONVERTED_FLAG,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
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
		COST_IND_COMPILED_SET_ID,
		REV_IND_COMPILED_SET_ID,
		INV_IND_COMPILED_SET_ID,
		COST_BURDEN_DISTRIBUTED_FLAG,
		IND_COST_DIST_REJECTION_CODE,
		ORIG_TRANSACTION_REFERENCE,
		TRANSACTION_SOURCE,
		PROJECT_ID,
		SOURCE_EXPENDITURE_ITEM_ID,
		JOB_ID,
		ORG_ID,
		SYSTEM_LINKAGE_FUNCTION,
		BURDEN_SUM_DEST_RUN_ID,
		RECEIPT_CURRENCY_AMOUNT,
		RECEIPT_CURRENCY_CODE,
		RECEIPT_EXCHANGE_RATE,
		DENOM_CURRENCY_CODE,
		DENOM_RAW_COST,
		DENOM_BURDENED_COST,
		ACCT_CURRENCY_CODE,
		ACCT_RATE_DATE,
		ACCT_RATE_TYPE,
		ACCT_EXCHANGE_RATE,
		ACCT_RAW_COST,
		ACCT_BURDENED_COST,
		ACCT_EXCHANGE_ROUNDING_LIMIT,
		PROJECT_CURRENCY_CODE,
		PROJECT_RATE_DATE,
		PROJECT_RATE_TYPE,
		PROJECT_EXCHANGE_RATE,
		DENORM_ID,
		CC_CROSS_CHARGE_CODE,
		CC_PRVDR_ORGANIZATION_ID,
		CC_RECVR_ORGANIZATION_ID,
		CC_REJECTION_CODE,
		DENOM_TP_CURRENCY_CODE,
		DENOM_TRANSFER_PRICE,
		ACCT_TP_RATE_TYPE,
		ACCT_TP_RATE_DATE,
		ACCT_TP_EXCHANGE_RATE,
		ACCT_TRANSFER_PRICE,
		PROJACCT_TRANSFER_PRICE,
		CC_MARKUP_BASE_CODE,
		TP_BASE_AMOUNT,
		CC_CROSS_CHARGE_TYPE,
		RECVR_ORG_ID,
		CC_BL_DISTRIBUTED_CODE,
		CC_IC_PROCESSED_CODE,
		TP_IND_COMPILED_SET_ID,
		TP_BILL_RATE,
		TP_BILL_MARKUP_PERCENTAGE,
		TP_SCHEDULE_LINE_PERCENTAGE,
		TP_RULE_PERCENTAGE,
		CC_PRVDR_COST_RECLASS_CODE,
		CRL_ASSET_CREATION_STATUS_CODE,
		CRL_ASSET_CREATION_REJ_CODE,
		COST_JOB_ID,
		TP_JOB_ID,
		PROV_PROJ_BILL_JOB_ID,
		COST_DIST_WARNING_CODE,
		PROJECT_TP_RATE_DATE,
		PROJECT_TP_RATE_TYPE,
		PROJECT_TP_EXCHANGE_RATE,
		PROJFUNC_TP_RATE_DATE,
		PROJFUNC_TP_RATE_TYPE,
		PROJFUNC_TP_EXCHANGE_RATE,
		PROJFUNC_TRANSFER_PRICE,
		BILL_TRANS_FORECAST_CURR_CODE,
		BILL_TRANS_FORECAST_REVENUE,
		PROJFUNC_REV_RATE_DATE,
		PROJFUNC_REV_EXCHANGE_RATE,
		PROJFUNC_COST_RATE_TYPE,
		PROJFUNC_COST_RATE_DATE,
		PROJFUNC_COST_EXCHANGE_RATE,
		PROJECT_RAW_COST,
		PROJECT_BURDENED_COST,
		ASSIGNMENT_ID,
		WORK_TYPE_ID,
		PROJFUNC_RAW_REVENUE,
		PROJECT_BILL_AMOUNT,
		PROJFUNC_CURRENCY_CODE,
		PROJECT_RAW_REVENUE,
		PROJECT_TRANSFER_PRICE,
		TP_AMT_TYPE_CODE,
		BILL_TRANS_CURRENCY_CODE,
		BILL_TRANS_RAW_REVENUE,
		BILL_TRANS_BILL_AMOUNT,
		BILL_TRANS_ADJUSTED_REVENUE,
		REVPROC_CURRENCY_CODE,
		REVPROC_RATE_TYPE,
		REVPROC_RATE_DATE,
		REVPROC_EXCHANGE_RATE,
		INVPROC_CURRENCY_CODE,
		INVPROC_RATE_TYPE,
		INVPROC_RATE_DATE,
		DISCOUNT_PERCENTAGE,
		LABOR_MULTIPLIER,
		AMOUNT_CALCULATION_CODE,
		BILL_MARKUP_PERCENTAGE,
		RATE_SOURCE_ID,
		INVPROC_EXCHANGE_RATE,
		INV_GEN_REJECTION_CODE,
		PROJFUNC_BILL_AMOUNT,
		PROJECT_REV_RATE_TYPE,
		PROJECT_REV_RATE_DATE,
		PROJECT_REV_EXCHANGE_RATE,
		PROJFUNC_REV_RATE_TYPE,
		PROJFUNC_INV_RATE_TYPE,
		PROJFUNC_INV_RATE_DATE,
		PROJFUNC_INV_EXCHANGE_RATE,
		PROJECT_INV_RATE_TYPE,
		PROJECT_INV_RATE_DATE,
		PROJECT_INV_EXCHANGE_RATE,
		PROJFUNC_FCST_RATE_TYPE,
		PROJFUNC_FCST_RATE_DATE,
		PROJFUNC_FCST_EXCHANGE_RATE,
		PRVDR_ACCRUAL_DATE,
		RECVR_ACCRUAL_DATE,
		RATE_DISC_REASON_CODE,
		POSTED_DENOM_BURDENED_COST,
		POSTED_PROJECT_BURDENED_COST,
		POSTED_PROJFUNC_BURDENED_COST,
		POSTED_ACCT_BURDENED_COST,
		ADJUSTMENT_TYPE,
		CAPITAL_EVENT_ID,
		PO_LINE_ID,
		PO_PRICE_TYPE,
		WIP_RESOURCE_ID,
		INVENTORY_ITEM_ID,
		UNIT_OF_MEASURE,
		SRC_SYSTEM_LINKAGE_FUNCTION,
		DOCUMENT_HEADER_ID,
		DOCUMENT_DISTRIBUTION_ID,
		DOCUMENT_LINE_NUMBER,
		DOCUMENT_PAYMENT_ID,
		VENDOR_ID,
		DOCUMENT_TYPE,
		DOCUMENT_DISTRIBUTION_TYPE,
		HISTORICAL_FLAG,
		LOCATION_ID,
		PAY_ELEMENT_TYPE_ID,
		COSTING_METHOD,
		RATE_SOURCE_CODE,
		PAYROLL_ACCRUAL_FLAG,
		RBC_ELEMENT_TYPE_ID,
		INTERFACE_RUN_ID,
		CBS_ELEMENT_ID,
		ADD_INV_GROUP,
		BILL_GROUP,
		LEGAL_ENTITY_ID,
		RECEIVER_LEGAL_ENTITY_ID,
		REVENUE_HOLD_FLAG,
		FCB_PROCESS_FLAG,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.pa_expenditure_items_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'pa_expenditure_items_all';
	
commit;