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

DROP TABLE if exists bec_ods.oe_transaction_types_all;

CREATE TABLE IF NOT EXISTS bec_ods.oe_transaction_types_all
(
	TRANSACTION_TYPE_ID NUMERIC(15,0) ENCODE az64 
	,TRANSACTION_TYPE_CODE VARCHAR(30) ENCODE lzo 
	,ORDER_CATEGORY_CODE VARCHAR(30) ENCODE lzo 
	,START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 	
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 
	,PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_ID NUMERIC(15,0) ENCODE az64 	
	,REQUEST_ID NUMERIC(15,0) ENCODE az64 
	,CURRENCY_CODE VARCHAR(30) ENCODE lzo 
	,CONVERSION_TYPE_CODE VARCHAR(30) ENCODE lzo 
	,CUST_TRX_TYPE_ID NUMERIC(15,0) ENCODE az64 
	,COST_OF_GOODS_SOLD_ACCOUNT NUMERIC(28,10) ENCODE az64 
	,ENTRY_CREDIT_CHECK_RULE_ID NUMERIC(15,0) ENCODE az64 
	,SHIPPING_CREDIT_CHECK_RULE_ID NUMERIC(15,0) ENCODE az64 
	,PRICE_LIST_ID NUMERIC(15,0) ENCODE az64 	
	,ENFORCE_LINE_PRICES_FLAG VARCHAR(1) ENCODE lzo 
	,WAREHOUSE_ID NUMERIC(15,0) ENCODE az64 	
	,DEMAND_CLASS_CODE VARCHAR(30) ENCODE lzo 
	,SHIPMENT_PRIORITY_CODE VARCHAR(30) ENCODE lzo 
	,SHIPPING_METHOD_CODE VARCHAR(30) ENCODE lzo 
	,FREIGHT_TERMS_CODE VARCHAR(30) ENCODE lzo 
	,FOB_POINT_CODE VARCHAR(30) ENCODE lzo 
	,SHIP_SOURCE_TYPE_CODE VARCHAR(30) ENCODE lzo 
	,AGREEMENT_TYPE_CODE VARCHAR(30) ENCODE lzo 
	,AGREEMENT_REQUIRED_FLAG VARCHAR(1) ENCODE lzo 
	,PO_REQUIRED_FLAG VARCHAR(1) ENCODE lzo
	,INVOICING_RULE_ID NUMERIC(15,0) ENCODE az64 
	,INVOICING_CREDIT_METHOD_CODE VARCHAR(30) ENCODE lzo 
	,ACCOUNTING_RULE_ID NUMERIC(15,0) ENCODE az64 
	,ACCOUNTING_CREDIT_METHOD_CODE VARCHAR(30) ENCODE lzo 
	,INVOICE_SOURCE_ID NUMERIC(15,0) ENCODE az64 
	,NON_DELIVERY_INVOICE_SOURCE_ID NUMERIC(15,0) ENCODE az64 
	,INSPECTION_REQUIRED_FLAG VARCHAR(1) ENCODE lzo 
	,DEPOT_REPAIR_CODE VARCHAR(30) ENCODE lzo 
	,ORG_ID NUMERIC(15,0) ENCODE az64
	,AUTO_SCHEDULING_FLAG VARCHAR(1) ENCODE lzo   
	,SCHEDULING_LEVEL_CODE VARCHAR(30) ENCODE lzo  
	,CONTEXT VARCHAR(30) ENCODE lzo 	
	,ATTRIBUTE1 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE2 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE3 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE4 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE5 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE6 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE7 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE8 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE9 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE10 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE11 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE12 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE13 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE14 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE15 VARCHAR(240) ENCODE lzo 
	,DEFAULT_INBOUND_LINE_TYPE_ID NUMERIC(15,0) ENCODE az64 
	,DEFAULT_OUTBOUND_LINE_TYPE_ID NUMERIC(15,0) ENCODE az64 
	,TAX_CALCULATION_EVENT_CODE VARCHAR(30) ENCODE lzo 
	,PICKING_CREDIT_CHECK_RULE_ID NUMERIC(15,0) ENCODE az64 
	,PACKING_CREDIT_CHECK_RULE_ID NUMERIC(15,0) ENCODE az64 
	,MIN_MARGIN_PERCENT NUMERIC(28,10) ENCODE az64 
	,SALES_DOCUMENT_TYPE_CODE VARCHAR(30) ENCODE lzo 
	,DEFAULT_LINE_SET_CODE VARCHAR(30) ENCODE lzo 
	,DEFAULT_FULFILLMENT_SET VARCHAR(1) ENCODE lzo 
	,DEF_TRANSACTION_PHASE_CODE VARCHAR(30) ENCODE lzo 
	,QUOTE_NUM_AS_ORD_NUM_FLAG VARCHAR(1) ENCODE lzo 
	,LAYOUT_TEMPLATE_ID NUMERIC(15,0) ENCODE az64 
	,CONTRACT_TEMPLATE_ID NUMERIC(15,0) ENCODE az64 
	,CREDIT_CARD_REV_REAUTH_CODE VARCHAR(30) ENCODE lzo 
	,USE_AME_APPROVAL VARCHAR(1) ENCODE lzo 
	,BILL_ONLY VARCHAR(1) ENCODE lzo  
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.oe_transaction_types_all (
	TRANSACTION_TYPE_ID,
	TRANSACTION_TYPE_CODE,
	ORDER_CATEGORY_CODE,
	START_DATE_ACTIVE,
	END_DATE_ACTIVE,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	REQUEST_ID,
	CURRENCY_CODE,
	CONVERSION_TYPE_CODE,
	CUST_TRX_TYPE_ID,
	COST_OF_GOODS_SOLD_ACCOUNT,
	ENTRY_CREDIT_CHECK_RULE_ID,
	SHIPPING_CREDIT_CHECK_RULE_ID,
	PRICE_LIST_ID,
	ENFORCE_LINE_PRICES_FLAG,
	WAREHOUSE_ID,
	DEMAND_CLASS_CODE,
	SHIPMENT_PRIORITY_CODE,
	SHIPPING_METHOD_CODE,
	FREIGHT_TERMS_CODE,
	FOB_POINT_CODE,
	SHIP_SOURCE_TYPE_CODE,
	AGREEMENT_TYPE_CODE,
	AGREEMENT_REQUIRED_FLAG,
	PO_REQUIRED_FLAG,
	INVOICING_RULE_ID,
	INVOICING_CREDIT_METHOD_CODE,
	ACCOUNTING_RULE_ID,
	ACCOUNTING_CREDIT_METHOD_CODE,
	INVOICE_SOURCE_ID,
	NON_DELIVERY_INVOICE_SOURCE_ID,
	INSPECTION_REQUIRED_FLAG,
	DEPOT_REPAIR_CODE,
	ORG_ID,
	AUTO_SCHEDULING_FLAG,
	SCHEDULING_LEVEL_CODE,
	CONTEXT,
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
	DEFAULT_INBOUND_LINE_TYPE_ID,
	DEFAULT_OUTBOUND_LINE_TYPE_ID,
	TAX_CALCULATION_EVENT_CODE,
	PICKING_CREDIT_CHECK_RULE_ID,
	PACKING_CREDIT_CHECK_RULE_ID,
	MIN_MARGIN_PERCENT,
	SALES_DOCUMENT_TYPE_CODE,
	DEFAULT_LINE_SET_CODE,
	DEFAULT_FULFILLMENT_SET,
	DEF_TRANSACTION_PHASE_CODE,
	QUOTE_NUM_AS_ORD_NUM_FLAG,
	LAYOUT_TEMPLATE_ID,
	CONTRACT_TEMPLATE_ID,
	CREDIT_CARD_REV_REAUTH_CODE,
	USE_AME_APPROVAL,
	BILL_ONLY, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
		TRANSACTION_TYPE_ID,
		TRANSACTION_TYPE_CODE,
		ORDER_CATEGORY_CODE,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		REQUEST_ID,
		CURRENCY_CODE,
		CONVERSION_TYPE_CODE,
		CUST_TRX_TYPE_ID,
		COST_OF_GOODS_SOLD_ACCOUNT,
		ENTRY_CREDIT_CHECK_RULE_ID,
		SHIPPING_CREDIT_CHECK_RULE_ID,
		PRICE_LIST_ID,
		ENFORCE_LINE_PRICES_FLAG,
		WAREHOUSE_ID,
		DEMAND_CLASS_CODE,
		SHIPMENT_PRIORITY_CODE,
		SHIPPING_METHOD_CODE,
		FREIGHT_TERMS_CODE,
		FOB_POINT_CODE,
		SHIP_SOURCE_TYPE_CODE,
		AGREEMENT_TYPE_CODE,
		AGREEMENT_REQUIRED_FLAG,
		PO_REQUIRED_FLAG,
		INVOICING_RULE_ID,
		INVOICING_CREDIT_METHOD_CODE,
		ACCOUNTING_RULE_ID,
		ACCOUNTING_CREDIT_METHOD_CODE,
		INVOICE_SOURCE_ID,
		NON_DELIVERY_INVOICE_SOURCE_ID,
		INSPECTION_REQUIRED_FLAG,
		DEPOT_REPAIR_CODE,
		ORG_ID,
		AUTO_SCHEDULING_FLAG,
		SCHEDULING_LEVEL_CODE,
		CONTEXT,
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
		DEFAULT_INBOUND_LINE_TYPE_ID,
		DEFAULT_OUTBOUND_LINE_TYPE_ID,
		TAX_CALCULATION_EVENT_CODE,
		PICKING_CREDIT_CHECK_RULE_ID,
		PACKING_CREDIT_CHECK_RULE_ID,
		MIN_MARGIN_PERCENT,
		SALES_DOCUMENT_TYPE_CODE,
		DEFAULT_LINE_SET_CODE,
		DEFAULT_FULFILLMENT_SET,
		DEF_TRANSACTION_PHASE_CODE,
		QUOTE_NUM_AS_ORD_NUM_FLAG,
		LAYOUT_TEMPLATE_ID,
		CONTRACT_TEMPLATE_ID,
		CREDIT_CARD_REV_REAUTH_CODE,
		USE_AME_APPROVAL,
		BILL_ONLY,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.oe_transaction_types_all;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oe_transaction_types_all';
	
commit;