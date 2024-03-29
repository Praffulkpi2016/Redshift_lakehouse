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

DROP TABLE if exists bec_ods.CSF_DEBRIEF_LINES;

CREATE TABLE IF NOT EXISTS bec_ods.CSF_DEBRIEF_LINES
(
	DEBRIEF_LINE_ID NUMERIC(15,0) ENCODE az64, 
	DEBRIEF_HEADER_ID NUMERIC(15,0) ENCODE az64, 
	DEBRIEF_LINE_NUMBER NUMERIC(15,0) ENCODE az64, 
	SERVICE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	TXN_BILLING_TYPE_ID NUMERIC(15,0) ENCODE az64, 
	INVENTORY_ITEM_ID NUMERIC(15,0) ENCODE az64, 
	ISSUING_INVENTORY_ORG_ID NUMERIC(15,0) ENCODE az64, 
	RECEIVING_INVENTORY_ORG_ID NUMERIC(15,0) ENCODE az64, 
	ISSUING_SUB_INVENTORY_CODE VARCHAR(10) ENCODE LZO, 
	RECEIVING_SUB_INVENTORY_CODE VARCHAR(10) ENCODE LZO, 
	ISSUING_LOCATOR_ID NUMERIC(15,0) ENCODE az64, 
	RECEIVING_LOCATOR_ID NUMERIC(15,0) ENCODE az64, 
	PARENT_PRODUCT_ID NUMERIC(15,0) ENCODE az64, 
	REMOVED_PRODUCT_ID NUMERIC(15,0) ENCODE az64, 
	STATUS_OF_RECEIVED_PART VARCHAR(30) ENCODE LZO, 
	ITEM_SERIAL_NUMBER VARCHAR(30) ENCODE LZO, 
	ITEM_REVISION VARCHAR(3) ENCODE LZO, 
	ITEM_LOTNUMBER VARCHAR(80) ENCODE LZO, 
	UOM_CODE VARCHAR(3) ENCODE LZO, 
	QUANTITY NUMERIC(28,10) ENCODE az64, 
	RMA_HEADER_ID NUMERIC(15,0) ENCODE az64, 
	DISPOSITION_CODE VARCHAR(30) ENCODE LZO, 
	MATERIAL_REASON_CODE VARCHAR(30) ENCODE LZO, 
	LABOR_REASON_CODE VARCHAR(30) ENCODE LZO, 
	EXPENSE_REASON_CODE VARCHAR(30) ENCODE LZO, 
	LABOR_START_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	LABOR_END_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	STARTING_MILEAGE NUMERIC(28,10) ENCODE az64, 
	ENDING_MILEAGE NUMERIC(28,10) ENCODE az64, 
	EXPENSE_AMOUNT NUMERIC(28,10) ENCODE az64, 
	CURRENCY_CODE VARCHAR(15) ENCODE LZO, 
	DEBRIEF_LINE_STATUS_ID NUMERIC(15,0) ENCODE az64, 
	CHANNEL_CODE VARCHAR(30) ENCODE LZO, 
	CHARGE_UPLOAD_STATUS VARCHAR(30) ENCODE LZO, 
	CHARGE_UPLOAD_MSG_CODE VARCHAR(30) ENCODE LZO, 
	CHARGE_UPLOAD_MESSAGE VARCHAR(240) ENCODE LZO, 
	IB_UPDATE_STATUS VARCHAR(30) ENCODE LZO, 
	IB_UPDATE_MSG_CODE VARCHAR(30) ENCODE LZO, 
	IB_UPDATE_MESSAGE VARCHAR(240) ENCODE LZO, 
	SPARE_UPDATE_STATUS VARCHAR(30) ENCODE LZO, 
	SPARE_UPDATE_MSG_CODE VARCHAR(30) ENCODE LZO, 
	SPARE_UPDATE_MESSAGE VARCHAR(240) ENCODE LZO, 
	CREATED_BY NUMERIC(15,0) ENCODE az64, 
	CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64, 
	LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64, 
	ATTRIBUTE1 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE2 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE3 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE4 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE5 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE6 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE7 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE8 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE9 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE10 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE11 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE12 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE13 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE14 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE15 VARCHAR(150) ENCODE LZO, 
	ATTRIBUTE_CATEGORY VARCHAR(150) ENCODE LZO, 
	SECURITY_GROUP_ID NUMERIC(15,0) ENCODE az64, 
	UPG_ORIG_SYSTEM_REF VARCHAR(60) ENCODE LZO, 
	UPG_ORIG_SYSTEM_REF_ID NUMERIC(15,0) ENCODE az64, 
	BUSINESS_PROCESS_ID NUMERIC(15,0) ENCODE az64, 
	RETURN_REASON_CODE VARCHAR(30) ENCODE LZO, 
	INSTANCE_ID NUMERIC(15,0) ENCODE az64, 
	STATISTICS_UPDATED VARCHAR(15) ENCODE LZO, 
	ERROR_TEXT VARCHAR(2000) ENCODE LZO, 
	TRANSACTION_TYPE_ID NUMERIC(15,0) ENCODE az64, 
	RETURN_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	OBJECT_VERSION_NUMBER NUMERIC(15,0) ENCODE az64, 
	ITEM_OPERATIONAL_STATUS_CODE VARCHAR(30) ENCODE LZO, 
	MATERIAL_TRANSACTION_ID NUMERIC(15,0) ENCODE az64, 
	USAGE_TYPE VARCHAR(30) ENCODE LZO, 
	RETURN_ORGANIZATION_ID NUMERIC(15,0) ENCODE az64, 
	RETURN_SUBINVENTORY_NAME VARCHAR(10) ENCODE LZO, 
	CARRIER_CODE VARCHAR(80) ENCODE LZO, 
	SHIPPING_METHOD VARCHAR(60) ENCODE LZO, 
	SHIPPING_NUMBER VARCHAR(60) ENCODE LZO, 
	WAYBILL VARCHAR(60) ENCODE LZO, 
	EXPENDITURE_ORG_ID NUMERIC(15,0) ENCODE az64, 
	PROJECT_ID NUMERIC(15,0) ENCODE az64, 
	PROJECT_TASK_ID NUMERIC(15,0) ENCODE az64,
	KCA_OPERATION VARCHAR(10)   ENCODE lzo,
    IS_DELETED_FLG VARCHAR(2) ENCODE lzo,
	kca_seq_id NUMERIC(36,0)   ENCODE az64,
	kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

COMMIT;

INSERT INTO bec_ods.CSF_DEBRIEF_LINES (
	DEBRIEF_LINE_ID, 
	DEBRIEF_HEADER_ID, 
	DEBRIEF_LINE_NUMBER, 
	SERVICE_DATE, 
	TXN_BILLING_TYPE_ID, 
	INVENTORY_ITEM_ID, 
	ISSUING_INVENTORY_ORG_ID, 
	RECEIVING_INVENTORY_ORG_ID, 
	ISSUING_SUB_INVENTORY_CODE, 
	RECEIVING_SUB_INVENTORY_CODE, 
	ISSUING_LOCATOR_ID, 
	RECEIVING_LOCATOR_ID, 
	PARENT_PRODUCT_ID, 
	REMOVED_PRODUCT_ID, 
	STATUS_OF_RECEIVED_PART, 
	ITEM_SERIAL_NUMBER, 
	ITEM_REVISION, 
	ITEM_LOTNUMBER, 
	UOM_CODE, 
	QUANTITY, 
	RMA_HEADER_ID, 
	DISPOSITION_CODE, 
	MATERIAL_REASON_CODE, 
	LABOR_REASON_CODE, 
	EXPENSE_REASON_CODE, 
	LABOR_START_DATE, 
	LABOR_END_DATE, 
	STARTING_MILEAGE, 
	ENDING_MILEAGE, 
	EXPENSE_AMOUNT, 
	CURRENCY_CODE, 
	DEBRIEF_LINE_STATUS_ID, 
	CHANNEL_CODE, 
	CHARGE_UPLOAD_STATUS, 
	CHARGE_UPLOAD_MSG_CODE, 
	CHARGE_UPLOAD_MESSAGE, 
	IB_UPDATE_STATUS, 
	IB_UPDATE_MSG_CODE, 
	IB_UPDATE_MESSAGE, 
	SPARE_UPDATE_STATUS, 
	SPARE_UPDATE_MSG_CODE, 
	SPARE_UPDATE_MESSAGE, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATED_BY, 
	LAST_UPDATE_DATE, 
	LAST_UPDATE_LOGIN, 
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
	ATTRIBUTE_CATEGORY, 
	SECURITY_GROUP_ID, 
	UPG_ORIG_SYSTEM_REF, 
	UPG_ORIG_SYSTEM_REF_ID, 
	BUSINESS_PROCESS_ID, 
	RETURN_REASON_CODE, 
	INSTANCE_ID, 
	STATISTICS_UPDATED, 
	ERROR_TEXT, 
	TRANSACTION_TYPE_ID, 
	RETURN_DATE, 
	OBJECT_VERSION_NUMBER, 
	ITEM_OPERATIONAL_STATUS_CODE, 
	MATERIAL_TRANSACTION_ID, 
	USAGE_TYPE, 
	RETURN_ORGANIZATION_ID, 
	RETURN_SUBINVENTORY_NAME, 
	CARRIER_CODE, 
	SHIPPING_METHOD, 
	SHIPPING_NUMBER, 
	WAYBILL, 
	EXPENDITURE_ORG_ID, 
	PROJECT_ID, 
	PROJECT_TASK_ID, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		DEBRIEF_LINE_ID, 
		DEBRIEF_HEADER_ID, 
		DEBRIEF_LINE_NUMBER, 
		SERVICE_DATE, 
		TXN_BILLING_TYPE_ID, 
		INVENTORY_ITEM_ID, 
		ISSUING_INVENTORY_ORG_ID, 
		RECEIVING_INVENTORY_ORG_ID, 
		ISSUING_SUB_INVENTORY_CODE, 
		RECEIVING_SUB_INVENTORY_CODE, 
		ISSUING_LOCATOR_ID, 
		RECEIVING_LOCATOR_ID, 
		PARENT_PRODUCT_ID, 
		REMOVED_PRODUCT_ID, 
		STATUS_OF_RECEIVED_PART, 
		ITEM_SERIAL_NUMBER, 
		ITEM_REVISION, 
		ITEM_LOTNUMBER, 
		UOM_CODE, 
		QUANTITY, 
		RMA_HEADER_ID, 
		DISPOSITION_CODE, 
		MATERIAL_REASON_CODE, 
		LABOR_REASON_CODE, 
		EXPENSE_REASON_CODE, 
		LABOR_START_DATE, 
		LABOR_END_DATE, 
		STARTING_MILEAGE, 
		ENDING_MILEAGE, 
		EXPENSE_AMOUNT, 
		CURRENCY_CODE, 
		DEBRIEF_LINE_STATUS_ID, 
		CHANNEL_CODE, 
		CHARGE_UPLOAD_STATUS, 
		CHARGE_UPLOAD_MSG_CODE, 
		CHARGE_UPLOAD_MESSAGE, 
		IB_UPDATE_STATUS, 
		IB_UPDATE_MSG_CODE, 
		IB_UPDATE_MESSAGE, 
		SPARE_UPDATE_STATUS, 
		SPARE_UPDATE_MSG_CODE, 
		SPARE_UPDATE_MESSAGE, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN, 
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
		ATTRIBUTE_CATEGORY, 
		SECURITY_GROUP_ID, 
		UPG_ORIG_SYSTEM_REF, 
		UPG_ORIG_SYSTEM_REF_ID, 
		BUSINESS_PROCESS_ID, 
		RETURN_REASON_CODE, 
		INSTANCE_ID, 
		STATISTICS_UPDATED, 
		ERROR_TEXT, 
		TRANSACTION_TYPE_ID, 
		RETURN_DATE, 
		OBJECT_VERSION_NUMBER, 
		ITEM_OPERATIONAL_STATUS_CODE, 
		MATERIAL_TRANSACTION_ID, 
		USAGE_TYPE, 
		RETURN_ORGANIZATION_ID, 
		RETURN_SUBINVENTORY_NAME, 
		CARRIER_CODE, 
		SHIPPING_METHOD, 
		SHIPPING_NUMBER, 
		WAYBILL, 
		EXPENDITURE_ORG_ID, 
		PROJECT_ID, 
		PROJECT_TASK_ID, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.CSF_DEBRIEF_LINES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'csf_debrief_lines';
	
commit;