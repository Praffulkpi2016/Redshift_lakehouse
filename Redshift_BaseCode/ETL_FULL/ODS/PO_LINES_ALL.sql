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
drop table if exists bec_ods.PO_LINES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_LINES_ALL
(
PO_LINE_ID	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,PO_HEADER_ID	NUMERIC(15,0)    ENCODE az64
,LINE_TYPE_ID	NUMERIC(15,0)    ENCODE az64
,LINE_NUM	NUMERIC(15,0)    ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)    ENCODE az64
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)    ENCODE az64
,ITEM_ID	NUMERIC(15,0)    ENCODE az64
,ITEM_REVISION	VARCHAR(3)   ENCODE lzo
,CATEGORY_ID	NUMERIC(15,0)   ENCODE az64
,ITEM_DESCRIPTION	VARCHAR(240)   ENCODE lzo
,UNIT_MEAS_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,QUANTITY_COMMITTED	NUMERIC(28,10)   ENCODE az64
,COMMITTED_AMOUNT	NUMERIC(28,10)   ENCODE az64
,ALLOW_PRICE_OVERRIDE_FLAG	VARCHAR(1)   ENCODE lzo
,NOT_TO_EXCEED_PRICE	NUMERIC(28,10)   ENCODE az64
,LIST_PRICE_PER_UNIT	NUMERIC(28,10)   ENCODE az64
,UNIT_PRICE	NUMERIC(28,10)   ENCODE az64
,QUANTITY	NUMERIC(28,10)   ENCODE az64
,UN_NUMBER_ID	NUMERIC(15,0)   ENCODE az64
,HAZARD_CLASS_ID	NUMERIC(15,0)   ENCODE az64
,NOTE_TO_VENDOR	VARCHAR(480)   ENCODE lzo
,FROM_HEADER_ID	NUMERIC(15,0)   ENCODE az64
,FROM_LINE_ID	NUMERIC(15,0)   ENCODE az64
,MIN_ORDER_QUANTITY	NUMERIC(28,10)   ENCODE az64
,MAX_ORDER_QUANTITY	NUMERIC(28,10)   ENCODE az64
,QTY_RCV_TOLERANCE	NUMERIC(28,10)   ENCODE az64
,OVER_TOLERANCE_ERROR_FLAG	VARCHAR(25)   ENCODE lzo
,MARKET_PRICE	NUMERIC(28,10)   ENCODE az64
,UNORDERED_FLAG	VARCHAR(1)   ENCODE lzo
,CLOSED_FLAG	VARCHAR(1)   ENCODE lzo
,USER_HOLD_FLAG	VARCHAR(1)   ENCODE lzo
,CANCEL_FLAG	VARCHAR(1)   ENCODE lzo
,CANCELLED_BY	NUMERIC(9,0)   ENCODE az64
,CANCEL_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CANCEL_REASON	VARCHAR(240)   ENCODE lzo
,FIRM_STATUS_LOOKUP_CODE	VARCHAR(30)   ENCODE lzo
,FIRM_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,VENDOR_PRODUCT_NUM	VARCHAR(25)   ENCODE lzo
,CONTRACT_NUM	VARCHAR(25)   ENCODE lzo
,TAXABLE_FLAG	VARCHAR(1)   ENCODE lzo
,TAX_NAME	VARCHAR(30)   ENCODE lzo
,TYPE_1099	VARCHAR(10)   ENCODE lzo
,CAPITAL_EXPENSE_FLAG	VARCHAR(1)   ENCODE lzo
,NEGOTIATED_BY_PREPARER_FLAG	VARCHAR(1)   ENCODE lzo
,ATTRIBUTE_CATEGORY	VARCHAR(30)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,REFERENCE_NUM	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,MIN_RELEASE_AMOUNT	NUMERIC(28,10)   ENCODE az64
,PRICE_TYPE_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,CLOSED_CODE	VARCHAR(25)   ENCODE lzo
,PRICE_BREAK_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,USSGL_TRANSACTION_CODE	VARCHAR(30)   ENCODE lzo
,GOVERNMENT_CONTEXT	VARCHAR(30)   ENCODE lzo
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CLOSED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CLOSED_REASON	VARCHAR(240)   ENCODE lzo
,CLOSED_BY	NUMERIC(9,0)   ENCODE az64
,TRANSACTION_REASON_CODE	VARCHAR(25)   ENCODE lzo
,ORG_ID	NUMERIC(15,0)  ENCODE az64
,QC_GRADE	VARCHAR(25)   ENCODE lzo
,BASE_UOM	VARCHAR(25)   ENCODE lzo
,BASE_QTY	NUMERIC(28,10)   ENCODE az64
,SECONDARY_UOM	VARCHAR(25)   ENCODE lzo
,SECONDARY_QTY	NUMERIC(28,10)   ENCODE az64
,GLOBAL_ATTRIBUTE_CATEGORY	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE16	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE17	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE18	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE19	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE20	VARCHAR(150)   ENCODE lzo
,LINE_REFERENCE_NUM	VARCHAR(25)   ENCODE lzo
,PROJECT_ID	NUMERIC(15,0)  ENCODE az64
,TASK_ID	NUMERIC(15,0)   ENCODE az64
,EXPIRATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,TAX_CODE_ID	NUMERIC(15,0)   ENCODE az64
,OKE_CONTRACT_HEADER_ID	NUMERIC(15,0)   ENCODE az64
,OKE_CONTRACT_VERSION_ID	NUMERIC(15,0)   ENCODE az64
,SECONDARY_QUANTITY	NUMERIC(28,10)   ENCODE az64
,SECONDARY_UNIT_OF_MEASURE	VARCHAR(25)   ENCODE lzo
,PREFERRED_GRADE	VARCHAR(150)   ENCODE lzo
,AUCTION_HEADER_ID	NUMERIC(15,0)  ENCODE az64
,AUCTION_DISPLAY_NUMBER	VARCHAR(40)   ENCODE lzo
,AUCTION_LINE_NUMBER	NUMERIC(15,0)   ENCODE az64
,BID_NUMBER	NUMERIC(15,0)   ENCODE az64
,BID_LINE_NUMBER	NUMERIC(15,0)   ENCODE az64
,RETROACTIVE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,SUPPLIER_REF_NUMBER	VARCHAR(150)   ENCODE lzo
,CONTRACT_ID	NUMERIC(15,0)   ENCODE az64
,START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,AMOUNT	NUMERIC(28,10)   ENCODE az64
,JOB_ID	NUMERIC(15,0)   ENCODE az64
,CONTRACTOR_FIRST_NAME	VARCHAR(240)   ENCODE lzo
,CONTRACTOR_LAST_NAME	VARCHAR(240)   ENCODE lzo
,FROM_LINE_LOCATION_ID	NUMERIC(15,0)   ENCODE az64
,ORDER_TYPE_LOOKUP_CODE	VARCHAR(25)   ENCODE lzo
,PURCHASE_BASIS	VARCHAR(30)   ENCODE lzo
,MATCHING_BASIS	VARCHAR(30)   ENCODE lzo
,SVC_AMOUNT_NOTIF_SENT	VARCHAR(1)   ENCODE lzo
,SVC_COMPLETION_NOTIF_SENT	VARCHAR(1)   ENCODE lzo
,BASE_UNIT_PRICE	NUMERIC(28,10)   ENCODE az64
,MANUAL_PRICE_CHANGE_FLAG	VARCHAR(1)   ENCODE lzo
,RETAINAGE_RATE	NUMERIC(28,10)   ENCODE az64
,MAX_RETAINAGE_AMOUNT	NUMERIC(28,10)   ENCODE az64
,PROGRESS_PAYMENT_RATE	NUMERIC(28,10)   ENCODE az64
,RECOUPMENT_RATE	NUMERIC(28,10)   ENCODE az64
,CATALOG_NAME	VARCHAR(255)   ENCODE lzo
,SUPPLIER_PART_AUXID	VARCHAR(255)   ENCODE lzo
,IP_CATEGORY_ID	NUMERIC(15,0)   ENCODE az64
,TAX_ATTRIBUTE_UPDATE_CODE	VARCHAR(15)   ENCODE lzo
,LAST_UPDATED_PROGRAM	VARCHAR(255)   ENCODE lzo
	,group_line_id NUMERIC(15,0)   ENCODE az64
	,line_num_display VARCHAR(240)   ENCODE lzo
	,clm_info_flag VARCHAR(1)   ENCODE lzo
	,clm_option_indicator VARCHAR(1)   ENCODE lzo
	,clm_base_line_num NUMERIC(15,0)   ENCODE az64
	,clm_option_num NUMERIC(15,0)   ENCODE az64
	,clm_option_from_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,clm_option_to_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,clm_funded_flag VARCHAR(1)   ENCODE lzo
	,contract_type VARCHAR(240)   ENCODE lzo
	,cost_constraint VARCHAR(240)   ENCODE lzo
	,clm_idc_type VARCHAR(240)   ENCODE lzo
	,uda_template_id NUMERIC(15,0)   ENCODE az64
	,user_document_status VARCHAR(30)   ENCODE lzo
	,draft_id NUMERIC(15,0)   ENCODE az64
	,clm_exercised_flag VARCHAR(1)   ENCODE lzo
	,clm_exercised_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,clm_min_total_amount NUMERIC(28,10)   ENCODE az64
	,clm_max_total_amount NUMERIC(28,10)   ENCODE az64
	,clm_min_total_quantity NUMERIC(15,0)   ENCODE az64
	,clm_max_total_quantity NUMERIC(15,0)   ENCODE az64
	,clm_min_order_amount NUMERIC(28,10)   ENCODE az64
	,clm_max_order_amount NUMERIC(28,10)   ENCODE az64
	,clm_min_order_quantity NUMERIC(15,0)   ENCODE az64
	,clm_max_order_quantity NUMERIC(15,0)   ENCODE az64
	,clm_total_amount_ordered NUMERIC(28,10)   ENCODE az64
	,clm_total_quantity_ordered NUMERIC(15,0)   ENCODE az64
	,clm_fsc_psc VARCHAR(200)   ENCODE lzo
	,clm_mdaps_mais VARCHAR(200)   ENCODE lzo
	,clm_naics VARCHAR(200)   ENCODE lzo
	,clm_order_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,clm_order_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,revision_num NUMERIC(15,0)   ENCODE az64
	,clm_approved_undef_amount NUMERIC(28,10)   ENCODE az64
	,clm_delivery_event_code VARCHAR(30)   ENCODE lzo
	,clm_exhibit_name VARCHAR(10)   ENCODE lzo
	,clm_payment_instr_code VARCHAR(30)   ENCODE lzo
	,clm_pop_exception_reason VARCHAR(4000)   ENCODE lzo
	,clm_uda_pricing_total NUMERIC(28,10)   ENCODE az64
	,clm_undef_action_code VARCHAR(10)   ENCODE lzo
	,clm_undef_flag VARCHAR(1)   ENCODE lzo
	,schedules_required_flag VARCHAR(1)   ENCODE lzo
	,"cancel_reason#1" VARCHAR(2000)   ENCODE lzo
	,"closed_reason#1" VARCHAR(2000)   ENCODE lzo
	--,igt_line_status VARCHAR(1)   ENCODE lzo
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.PO_LINES_ALL
(PO_LINE_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,PO_HEADER_ID
,LINE_TYPE_ID
,LINE_NUM
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,ITEM_ID
,ITEM_REVISION
,CATEGORY_ID
,ITEM_DESCRIPTION
,UNIT_MEAS_LOOKUP_CODE
,QUANTITY_COMMITTED
,COMMITTED_AMOUNT
,ALLOW_PRICE_OVERRIDE_FLAG
,NOT_TO_EXCEED_PRICE
,LIST_PRICE_PER_UNIT
,UNIT_PRICE
,QUANTITY
,UN_NUMBER_ID
,HAZARD_CLASS_ID
,NOTE_TO_VENDOR
,FROM_HEADER_ID
,FROM_LINE_ID
,MIN_ORDER_QUANTITY
,MAX_ORDER_QUANTITY
,QTY_RCV_TOLERANCE
,OVER_TOLERANCE_ERROR_FLAG
,MARKET_PRICE
,UNORDERED_FLAG
,CLOSED_FLAG
,USER_HOLD_FLAG
,CANCEL_FLAG
,CANCELLED_BY
,CANCEL_DATE
,CANCEL_REASON
,FIRM_STATUS_LOOKUP_CODE
,FIRM_DATE
,VENDOR_PRODUCT_NUM
,CONTRACT_NUM
,TAXABLE_FLAG
,TAX_NAME
,TYPE_1099
,CAPITAL_EXPENSE_FLAG
,NEGOTIATED_BY_PREPARER_FLAG
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,REFERENCE_NUM
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,MIN_RELEASE_AMOUNT
,PRICE_TYPE_LOOKUP_CODE
,CLOSED_CODE
,PRICE_BREAK_LOOKUP_CODE
,USSGL_TRANSACTION_CODE
,GOVERNMENT_CONTEXT
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,CLOSED_DATE
,CLOSED_REASON
,CLOSED_BY
,TRANSACTION_REASON_CODE
,ORG_ID
,QC_GRADE
,BASE_UOM
,BASE_QTY
,SECONDARY_UOM
,SECONDARY_QTY
,GLOBAL_ATTRIBUTE_CATEGORY
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
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
,GLOBAL_ATTRIBUTE20
,LINE_REFERENCE_NUM
,PROJECT_ID
,TASK_ID
,EXPIRATION_DATE
,TAX_CODE_ID
,OKE_CONTRACT_HEADER_ID
,OKE_CONTRACT_VERSION_ID
,SECONDARY_QUANTITY
,SECONDARY_UNIT_OF_MEASURE
,PREFERRED_GRADE
,AUCTION_HEADER_ID
,AUCTION_DISPLAY_NUMBER
,AUCTION_LINE_NUMBER
,BID_NUMBER
,BID_LINE_NUMBER
,RETROACTIVE_DATE
,SUPPLIER_REF_NUMBER
,CONTRACT_ID
,START_DATE
,AMOUNT
,JOB_ID
,CONTRACTOR_FIRST_NAME
,CONTRACTOR_LAST_NAME
,FROM_LINE_LOCATION_ID
,ORDER_TYPE_LOOKUP_CODE
,PURCHASE_BASIS
,MATCHING_BASIS
,SVC_AMOUNT_NOTIF_SENT
,SVC_COMPLETION_NOTIF_SENT
,BASE_UNIT_PRICE
,MANUAL_PRICE_CHANGE_FLAG
,RETAINAGE_RATE
,MAX_RETAINAGE_AMOUNT
,PROGRESS_PAYMENT_RATE
,RECOUPMENT_RATE
,CATALOG_NAME
,SUPPLIER_PART_AUXID
,IP_CATEGORY_ID
,TAX_ATTRIBUTE_UPDATE_CODE
,LAST_UPDATED_PROGRAM,
	group_line_id,
	line_num_display,
	clm_info_flag,
	clm_option_indicator,
	clm_base_line_num,
	clm_option_num,
	clm_option_from_date,
	clm_option_to_date,
	clm_funded_flag,
	contract_type,
	cost_constraint,
	clm_idc_type,
	uda_template_id,
	user_document_status,
	draft_id,
	clm_exercised_flag,
	clm_exercised_date,
	clm_min_total_amount,
	clm_max_total_amount,
	clm_min_total_quantity,
	clm_max_total_quantity,
	clm_min_order_amount,
	clm_max_order_amount,
	clm_min_order_quantity,
	clm_max_order_quantity,
	clm_total_amount_ordered,
	clm_total_quantity_ordered,
	clm_fsc_psc,
	clm_mdaps_mais,
	clm_naics,
	clm_order_start_date,
	clm_order_end_date,
	revision_num,
	clm_approved_undef_amount,
	clm_delivery_event_code,
	clm_exhibit_name,
	clm_payment_instr_code,
	clm_pop_exception_reason,
	clm_uda_pricing_total,
	clm_undef_action_code,
	clm_undef_flag,
	schedules_required_flag,
	"cancel_reason#1",
	"closed_reason#1"
	--igt_line_status
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(
select
PO_LINE_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,PO_HEADER_ID
,LINE_TYPE_ID
,LINE_NUM
,LAST_UPDATE_LOGIN
,CREATION_DATE
,CREATED_BY
,ITEM_ID
,ITEM_REVISION
,CATEGORY_ID
,ITEM_DESCRIPTION
,UNIT_MEAS_LOOKUP_CODE
,QUANTITY_COMMITTED
,COMMITTED_AMOUNT
,ALLOW_PRICE_OVERRIDE_FLAG
,NOT_TO_EXCEED_PRICE
,LIST_PRICE_PER_UNIT
,UNIT_PRICE
,QUANTITY
,UN_NUMBER_ID
,HAZARD_CLASS_ID
,NOTE_TO_VENDOR
,FROM_HEADER_ID
,FROM_LINE_ID
,MIN_ORDER_QUANTITY
,MAX_ORDER_QUANTITY
,QTY_RCV_TOLERANCE
,OVER_TOLERANCE_ERROR_FLAG
,MARKET_PRICE
,UNORDERED_FLAG
,CLOSED_FLAG
,USER_HOLD_FLAG
,CANCEL_FLAG
,CANCELLED_BY
,CANCEL_DATE
,CANCEL_REASON
,FIRM_STATUS_LOOKUP_CODE
,FIRM_DATE
,VENDOR_PRODUCT_NUM
,CONTRACT_NUM
,TAXABLE_FLAG
,TAX_NAME
,TYPE_1099
,CAPITAL_EXPENSE_FLAG
,NEGOTIATED_BY_PREPARER_FLAG
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,REFERENCE_NUM
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,MIN_RELEASE_AMOUNT
,PRICE_TYPE_LOOKUP_CODE
,CLOSED_CODE
,PRICE_BREAK_LOOKUP_CODE
,USSGL_TRANSACTION_CODE
,GOVERNMENT_CONTEXT
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,CLOSED_DATE
,CLOSED_REASON
,CLOSED_BY
,TRANSACTION_REASON_CODE
,ORG_ID
,QC_GRADE
,BASE_UOM
,BASE_QTY
,SECONDARY_UOM
,SECONDARY_QTY
,GLOBAL_ATTRIBUTE_CATEGORY
,GLOBAL_ATTRIBUTE1
,GLOBAL_ATTRIBUTE2
,GLOBAL_ATTRIBUTE3
,GLOBAL_ATTRIBUTE4
,GLOBAL_ATTRIBUTE5
,GLOBAL_ATTRIBUTE6
,GLOBAL_ATTRIBUTE7
,GLOBAL_ATTRIBUTE8
,GLOBAL_ATTRIBUTE9
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
,GLOBAL_ATTRIBUTE20
,LINE_REFERENCE_NUM
,PROJECT_ID
,TASK_ID
,EXPIRATION_DATE
,TAX_CODE_ID
,OKE_CONTRACT_HEADER_ID
,OKE_CONTRACT_VERSION_ID
,SECONDARY_QUANTITY
,SECONDARY_UNIT_OF_MEASURE
,PREFERRED_GRADE
,AUCTION_HEADER_ID
,AUCTION_DISPLAY_NUMBER
,AUCTION_LINE_NUMBER
,BID_NUMBER
,BID_LINE_NUMBER
,RETROACTIVE_DATE
,SUPPLIER_REF_NUMBER
,CONTRACT_ID
,START_DATE
,AMOUNT
,JOB_ID
,CONTRACTOR_FIRST_NAME
,CONTRACTOR_LAST_NAME
,FROM_LINE_LOCATION_ID
,ORDER_TYPE_LOOKUP_CODE
,PURCHASE_BASIS
,MATCHING_BASIS
,SVC_AMOUNT_NOTIF_SENT
,SVC_COMPLETION_NOTIF_SENT
,BASE_UNIT_PRICE
,MANUAL_PRICE_CHANGE_FLAG
,RETAINAGE_RATE
,MAX_RETAINAGE_AMOUNT
,PROGRESS_PAYMENT_RATE
,RECOUPMENT_RATE
,CATALOG_NAME
,SUPPLIER_PART_AUXID
,IP_CATEGORY_ID
,TAX_ATTRIBUTE_UPDATE_CODE
,LAST_UPDATED_PROGRAM,
	group_line_id,
	line_num_display,
	clm_info_flag,
	clm_option_indicator,
	clm_base_line_num,
	clm_option_num,
	clm_option_from_date,
	clm_option_to_date,
	clm_funded_flag,
	contract_type,
	cost_constraint,
	clm_idc_type,
	uda_template_id,
	user_document_status,
	draft_id,
	clm_exercised_flag,
	clm_exercised_date,
	clm_min_total_amount,
	clm_max_total_amount,
	clm_min_total_quantity,
	clm_max_total_quantity,
	clm_min_order_amount,
	clm_max_order_amount,
	clm_min_order_quantity,
	clm_max_order_quantity,
	clm_total_amount_ordered,
	clm_total_quantity_ordered,
	clm_fsc_psc,
	clm_mdaps_mais,
	clm_naics,
	clm_order_start_date,
	clm_order_end_date,
	revision_num,
	clm_approved_undef_amount,
	clm_delivery_event_code,
	clm_exhibit_name,
	clm_payment_instr_code,
	clm_pop_exception_reason,
	clm_uda_pricing_total,
	clm_undef_action_code,
	clm_undef_flag,
	schedules_required_flag,
	"cancel_reason#1",
	"closed_reason#1",
	--igt_line_status
KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID 
,kca_seq_date
from bec_ods_stg.PO_LINES_ALL
);

end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='po_lines_all'; 

commit;