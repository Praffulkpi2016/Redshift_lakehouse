/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.RCV_TRANSACTIONS_INTERFACE
where INTERFACE_TRANSACTION_ID in (
select stg.INTERFACE_TRANSACTION_ID from bec_ods.RCV_TRANSACTIONS_INTERFACE ods, bec_ods_stg.RCV_TRANSACTIONS_INTERFACE stg
where ods.INTERFACE_TRANSACTION_ID = stg.INTERFACE_TRANSACTION_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RCV_TRANSACTIONS_INTERFACE
       (
	  INTERFACE_TRANSACTION_ID
,      GROUP_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      TRANSACTION_TYPE
,      TRANSACTION_DATE
,      PROCESSING_STATUS_CODE
,      PROCESSING_MODE_CODE
,      PROCESSING_REQUEST_ID
,      TRANSACTION_STATUS_CODE
,      CATEGORY_ID
,      QUANTITY
,      UNIT_OF_MEASURE
,      INTERFACE_SOURCE_CODE
,      INTERFACE_SOURCE_LINE_ID
,      INV_TRANSACTION_ID
,      ITEM_ID
,      ITEM_DESCRIPTION
,      ITEM_REVISION
,      UOM_CODE
,      EMPLOYEE_ID
,      AUTO_TRANSACT_CODE
,      SHIPMENT_HEADER_ID
,      SHIPMENT_LINE_ID
,      SHIP_TO_LOCATION_ID
,      PRIMARY_QUANTITY
,      PRIMARY_UNIT_OF_MEASURE
,      RECEIPT_SOURCE_CODE
,      VENDOR_ID
,      VENDOR_SITE_ID
,      FROM_ORGANIZATION_ID
,      FROM_SUBINVENTORY
,      TO_ORGANIZATION_ID
,      INTRANSIT_OWNING_ORG_ID
,      ROUTING_HEADER_ID
,      ROUTING_STEP_ID
,      SOURCE_DOCUMENT_CODE
,      PARENT_TRANSACTION_ID
,      PO_HEADER_ID
,      PO_REVISION_NUM
,      PO_RELEASE_ID
,      PO_LINE_ID
,      PO_LINE_LOCATION_ID
,      PO_UNIT_PRICE
,      CURRENCY_CODE
,      CURRENCY_CONVERSION_TYPE
,      CURRENCY_CONVERSION_RATE
,      CURRENCY_CONVERSION_DATE
,      PO_DISTRIBUTION_ID
,      REQUISITION_LINE_ID
,      REQ_DISTRIBUTION_ID
,      CHARGE_ACCOUNT_ID
,      SUBSTITUTE_UNORDERED_CODE
,      RECEIPT_EXCEPTION_FLAG
,      ACCRUAL_STATUS_CODE
,      INSPECTION_STATUS_CODE
,      INSPECTION_QUALITY_CODE
,      DESTINATION_TYPE_CODE
,      DELIVER_TO_PERSON_ID
,      LOCATION_ID
,      DELIVER_TO_LOCATION_ID
,      SUBINVENTORY
,      LOCATOR_ID
,      WIP_ENTITY_ID
,      WIP_LINE_ID
,      DEPARTMENT_CODE
,      WIP_REPETITIVE_SCHEDULE_ID
,      WIP_OPERATION_SEQ_NUM
,      WIP_RESOURCE_SEQ_NUM
,      BOM_RESOURCE_ID
,      SHIPMENT_NUM
,      FREIGHT_CARRIER_CODE
,      BILL_OF_LADING
,      PACKING_SLIP
,      SHIPPED_DATE
,      EXPECTED_RECEIPT_DATE
,      ACTUAL_COST
,      TRANSFER_COST
,      TRANSPORTATION_COST
,      TRANSPORTATION_ACCOUNT_ID
,      NUM_OF_CONTAINERS
,      WAYBILL_AIRBILL_NUM
,      VENDOR_ITEM_NUM
,      VENDOR_LOT_NUM
,      RMA_REFERENCE
,      COMMENTS
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      SHIP_HEAD_ATTRIBUTE_CATEGORY
,      SHIP_HEAD_ATTRIBUTE1
,      SHIP_HEAD_ATTRIBUTE2
,      SHIP_HEAD_ATTRIBUTE3
,      SHIP_HEAD_ATTRIBUTE4
,      SHIP_HEAD_ATTRIBUTE5
,      SHIP_HEAD_ATTRIBUTE6
,      SHIP_HEAD_ATTRIBUTE7
,      SHIP_HEAD_ATTRIBUTE8
,      SHIP_HEAD_ATTRIBUTE9
,      SHIP_HEAD_ATTRIBUTE10
,      SHIP_HEAD_ATTRIBUTE11
,      SHIP_HEAD_ATTRIBUTE12
,      SHIP_HEAD_ATTRIBUTE13
,      SHIP_HEAD_ATTRIBUTE14
,      SHIP_HEAD_ATTRIBUTE15
,      SHIP_LINE_ATTRIBUTE_CATEGORY
,      SHIP_LINE_ATTRIBUTE1
,      SHIP_LINE_ATTRIBUTE2
,      SHIP_LINE_ATTRIBUTE3
,      SHIP_LINE_ATTRIBUTE4
,      SHIP_LINE_ATTRIBUTE5
,      SHIP_LINE_ATTRIBUTE6
,      SHIP_LINE_ATTRIBUTE7
,      SHIP_LINE_ATTRIBUTE8
,      SHIP_LINE_ATTRIBUTE9
,      SHIP_LINE_ATTRIBUTE10
,      SHIP_LINE_ATTRIBUTE11
,      SHIP_LINE_ATTRIBUTE12
,      SHIP_LINE_ATTRIBUTE13
,      SHIP_LINE_ATTRIBUTE14
,      SHIP_LINE_ATTRIBUTE15
,      USSGL_TRANSACTION_CODE
,      GOVERNMENT_CONTEXT
,      REASON_ID
,      DESTINATION_CONTEXT
,      SOURCE_DOC_QUANTITY
,      SOURCE_DOC_UNIT_OF_MEASURE
,      MOVEMENT_ID
,      HEADER_INTERFACE_ID
,      VENDOR_CUM_SHIPPED_QTY
,      ITEM_NUM
,      DOCUMENT_NUM
,      DOCUMENT_LINE_NUM
,      TRUCK_NUM
,      SHIP_TO_LOCATION_CODE
,      CONTAINER_NUM
,      SUBSTITUTE_ITEM_NUM
,      NOTICE_UNIT_PRICE
,      ITEM_CATEGORY
,      LOCATION_CODE
,      VENDOR_NAME
,      VENDOR_NUM
,      VENDOR_SITE_CODE
,      FROM_ORGANIZATION_CODE
,      TO_ORGANIZATION_CODE
,      INTRANSIT_OWNING_ORG_CODE
,      ROUTING_CODE
,      ROUTING_STEP
,      RELEASE_NUM
,      DOCUMENT_SHIPMENT_LINE_NUM
,      DOCUMENT_DISTRIBUTION_NUM
,      DELIVER_TO_PERSON_NAME
,      DELIVER_TO_LOCATION_CODE
,      USE_MTL_LOT
,      USE_MTL_SERIAL
,      LOCATOR
,      REASON_NAME
,      VALIDATION_FLAG
,      SUBSTITUTE_ITEM_ID
,      QUANTITY_SHIPPED
,      QUANTITY_INVOICED
,      TAX_NAME
,      TAX_AMOUNT
,      REQ_NUM
,      REQ_LINE_NUM
,      REQ_DISTRIBUTION_NUM
,      WIP_ENTITY_NAME
,      WIP_LINE_CODE
,      RESOURCE_CODE
,      SHIPMENT_LINE_STATUS_CODE
,      BARCODE_LABEL
,      TRANSFER_PERCENTAGE
,      QA_COLLECTION_ID
,      COUNTRY_OF_ORIGIN_CODE
,      OE_ORDER_HEADER_ID
,      OE_ORDER_LINE_ID
,      CUSTOMER_ID
,      CUSTOMER_SITE_ID
,      CUSTOMER_ITEM_NUM
,      CREATE_DEBIT_MEMO_FLAG
,      PUT_AWAY_RULE_ID
,      PUT_AWAY_STRATEGY_ID
,      LPN_ID
,      TRANSFER_LPN_ID
,      COST_GROUP_ID
,      MOBILE_TXN
,      MMTT_TEMP_ID
,      TRANSFER_COST_GROUP_ID
,      SECONDARY_QUANTITY
,      SECONDARY_UNIT_OF_MEASURE
,      SECONDARY_UOM_CODE
,      QC_GRADE
,      FROM_LOCATOR
,      FROM_LOCATOR_ID
,      PARENT_SOURCE_TRANSACTION_NUM
,      INTERFACE_AVAILABLE_QTY
,      INTERFACE_TRANSACTION_QTY
,      INTERFACE_AVAILABLE_AMT
,      INTERFACE_TRANSACTION_AMT
,      LICENSE_PLATE_NUMBER
,      SOURCE_TRANSACTION_NUM
,      TRANSFER_LICENSE_PLATE_NUMBER
,      LPN_GROUP_ID
,      ORDER_TRANSACTION_ID
,      CUSTOMER_ACCOUNT_NUMBER
,      CUSTOMER_PARTY_NAME
,      OE_ORDER_LINE_NUM
,      OE_ORDER_NUM
,      PARENT_INTERFACE_TXN_ID
,      CUSTOMER_ITEM_ID
,      AMOUNT
,      JOB_ID
,      TIMECARD_ID
,      TIMECARD_OVN
,      ERECORD_ID
,      PROJECT_ID
,      TASK_ID
,      ASN_ATTACH_ID
,      ORG_ID
,      OPERATING_UNIT
,      REQUESTED_AMOUNT
,      MATERIAL_STORED_AMOUNT
,      AMOUNT_SHIPPED
,      MATCHING_BASIS
,      REPLENISH_ORDER_LINE_ID
,      EXPRESS_TRANSACTION
,      LCM_SHIPMENT_LINE_ID
,      UNIT_LANDED_COST
,      LCM_ADJUSTMENT_NUM
 ,       KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		INTERFACE_TRANSACTION_ID
,      GROUP_ID
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      TRANSACTION_TYPE
,      TRANSACTION_DATE
,      PROCESSING_STATUS_CODE
,      PROCESSING_MODE_CODE
,      PROCESSING_REQUEST_ID
,      TRANSACTION_STATUS_CODE
,      CATEGORY_ID
,      QUANTITY
,      UNIT_OF_MEASURE
,      INTERFACE_SOURCE_CODE
,      INTERFACE_SOURCE_LINE_ID
,      INV_TRANSACTION_ID
,      ITEM_ID
,      ITEM_DESCRIPTION
,      ITEM_REVISION
,      UOM_CODE
,      EMPLOYEE_ID
,      AUTO_TRANSACT_CODE
,      SHIPMENT_HEADER_ID
,      SHIPMENT_LINE_ID
,      SHIP_TO_LOCATION_ID
,      PRIMARY_QUANTITY
,      PRIMARY_UNIT_OF_MEASURE
,      RECEIPT_SOURCE_CODE
,      VENDOR_ID
,      VENDOR_SITE_ID
,      FROM_ORGANIZATION_ID
,      FROM_SUBINVENTORY
,      TO_ORGANIZATION_ID
,      INTRANSIT_OWNING_ORG_ID
,      ROUTING_HEADER_ID
,      ROUTING_STEP_ID
,      SOURCE_DOCUMENT_CODE
,      PARENT_TRANSACTION_ID
,      PO_HEADER_ID
,      PO_REVISION_NUM
,      PO_RELEASE_ID
,      PO_LINE_ID
,      PO_LINE_LOCATION_ID
,      PO_UNIT_PRICE
,      CURRENCY_CODE
,      CURRENCY_CONVERSION_TYPE
,      CURRENCY_CONVERSION_RATE
,      CURRENCY_CONVERSION_DATE
,      PO_DISTRIBUTION_ID
,      REQUISITION_LINE_ID
,      REQ_DISTRIBUTION_ID
,      CHARGE_ACCOUNT_ID
,      SUBSTITUTE_UNORDERED_CODE
,      RECEIPT_EXCEPTION_FLAG
,      ACCRUAL_STATUS_CODE
,      INSPECTION_STATUS_CODE
,      INSPECTION_QUALITY_CODE
,      DESTINATION_TYPE_CODE
,      DELIVER_TO_PERSON_ID
,      LOCATION_ID
,      DELIVER_TO_LOCATION_ID
,      SUBINVENTORY
,      LOCATOR_ID
,      WIP_ENTITY_ID
,      WIP_LINE_ID
,      DEPARTMENT_CODE
,      WIP_REPETITIVE_SCHEDULE_ID
,      WIP_OPERATION_SEQ_NUM
,      WIP_RESOURCE_SEQ_NUM
,      BOM_RESOURCE_ID
,      SHIPMENT_NUM
,      FREIGHT_CARRIER_CODE
,      BILL_OF_LADING
,      PACKING_SLIP
,      SHIPPED_DATE
,      EXPECTED_RECEIPT_DATE
,      ACTUAL_COST
,      TRANSFER_COST
,      TRANSPORTATION_COST
,      TRANSPORTATION_ACCOUNT_ID
,      NUM_OF_CONTAINERS
,      WAYBILL_AIRBILL_NUM
,      VENDOR_ITEM_NUM
,      VENDOR_LOT_NUM
,      RMA_REFERENCE
,      COMMENTS
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      SHIP_HEAD_ATTRIBUTE_CATEGORY
,      SHIP_HEAD_ATTRIBUTE1
,      SHIP_HEAD_ATTRIBUTE2
,      SHIP_HEAD_ATTRIBUTE3
,      SHIP_HEAD_ATTRIBUTE4
,      SHIP_HEAD_ATTRIBUTE5
,      SHIP_HEAD_ATTRIBUTE6
,      SHIP_HEAD_ATTRIBUTE7
,      SHIP_HEAD_ATTRIBUTE8
,      SHIP_HEAD_ATTRIBUTE9
,      SHIP_HEAD_ATTRIBUTE10
,      SHIP_HEAD_ATTRIBUTE11
,      SHIP_HEAD_ATTRIBUTE12
,      SHIP_HEAD_ATTRIBUTE13
,      SHIP_HEAD_ATTRIBUTE14
,      SHIP_HEAD_ATTRIBUTE15
,      SHIP_LINE_ATTRIBUTE_CATEGORY
,      SHIP_LINE_ATTRIBUTE1
,      SHIP_LINE_ATTRIBUTE2
,      SHIP_LINE_ATTRIBUTE3
,      SHIP_LINE_ATTRIBUTE4
,      SHIP_LINE_ATTRIBUTE5
,      SHIP_LINE_ATTRIBUTE6
,      SHIP_LINE_ATTRIBUTE7
,      SHIP_LINE_ATTRIBUTE8
,      SHIP_LINE_ATTRIBUTE9
,      SHIP_LINE_ATTRIBUTE10
,      SHIP_LINE_ATTRIBUTE11
,      SHIP_LINE_ATTRIBUTE12
,      SHIP_LINE_ATTRIBUTE13
,      SHIP_LINE_ATTRIBUTE14
,      SHIP_LINE_ATTRIBUTE15
,      USSGL_TRANSACTION_CODE
,      GOVERNMENT_CONTEXT
,      REASON_ID
,      DESTINATION_CONTEXT
,      SOURCE_DOC_QUANTITY
,      SOURCE_DOC_UNIT_OF_MEASURE
,      MOVEMENT_ID
,      HEADER_INTERFACE_ID
,      VENDOR_CUM_SHIPPED_QTY
,      ITEM_NUM
,      DOCUMENT_NUM
,      DOCUMENT_LINE_NUM
,      TRUCK_NUM
,      SHIP_TO_LOCATION_CODE
,      CONTAINER_NUM
,      SUBSTITUTE_ITEM_NUM
,      NOTICE_UNIT_PRICE
,      ITEM_CATEGORY
,      LOCATION_CODE
,      VENDOR_NAME
,      VENDOR_NUM
,      VENDOR_SITE_CODE
,      FROM_ORGANIZATION_CODE
,      TO_ORGANIZATION_CODE
,      INTRANSIT_OWNING_ORG_CODE
,      ROUTING_CODE
,      ROUTING_STEP
,      RELEASE_NUM
,      DOCUMENT_SHIPMENT_LINE_NUM
,      DOCUMENT_DISTRIBUTION_NUM
,      DELIVER_TO_PERSON_NAME
,      DELIVER_TO_LOCATION_CODE
,      USE_MTL_LOT
,      USE_MTL_SERIAL
,      LOCATOR
,      REASON_NAME
,      VALIDATION_FLAG
,      SUBSTITUTE_ITEM_ID
,      QUANTITY_SHIPPED
,      QUANTITY_INVOICED
,      TAX_NAME
,      TAX_AMOUNT
,      REQ_NUM
,      REQ_LINE_NUM
,      REQ_DISTRIBUTION_NUM
,      WIP_ENTITY_NAME
,      WIP_LINE_CODE
,      RESOURCE_CODE
,      SHIPMENT_LINE_STATUS_CODE
,      BARCODE_LABEL
,      TRANSFER_PERCENTAGE
,      QA_COLLECTION_ID
,      COUNTRY_OF_ORIGIN_CODE
,      OE_ORDER_HEADER_ID
,      OE_ORDER_LINE_ID
,      CUSTOMER_ID
,      CUSTOMER_SITE_ID
,      CUSTOMER_ITEM_NUM
,      CREATE_DEBIT_MEMO_FLAG
,      PUT_AWAY_RULE_ID
,      PUT_AWAY_STRATEGY_ID
,      LPN_ID
,      TRANSFER_LPN_ID
,      COST_GROUP_ID
,      MOBILE_TXN
,      MMTT_TEMP_ID
,      TRANSFER_COST_GROUP_ID
,      SECONDARY_QUANTITY
,      SECONDARY_UNIT_OF_MEASURE
,      SECONDARY_UOM_CODE
,      QC_GRADE
,      FROM_LOCATOR
,      FROM_LOCATOR_ID
,      PARENT_SOURCE_TRANSACTION_NUM
,      INTERFACE_AVAILABLE_QTY
,      INTERFACE_TRANSACTION_QTY
,      INTERFACE_AVAILABLE_AMT
,      INTERFACE_TRANSACTION_AMT
,      LICENSE_PLATE_NUMBER
,      SOURCE_TRANSACTION_NUM
,      TRANSFER_LICENSE_PLATE_NUMBER
,      LPN_GROUP_ID
,      ORDER_TRANSACTION_ID
,      CUSTOMER_ACCOUNT_NUMBER
,      CUSTOMER_PARTY_NAME
,      OE_ORDER_LINE_NUM
,      OE_ORDER_NUM
,      PARENT_INTERFACE_TXN_ID
,      CUSTOMER_ITEM_ID
,      AMOUNT
,      JOB_ID
,      TIMECARD_ID
,      TIMECARD_OVN
,      ERECORD_ID
,      PROJECT_ID
,      TASK_ID
,      ASN_ATTACH_ID
,      ORG_ID
,      OPERATING_UNIT
,      REQUESTED_AMOUNT
,      MATERIAL_STORED_AMOUNT
,      AMOUNT_SHIPPED
,      MATCHING_BASIS
,      REPLENISH_ORDER_LINE_ID
,      EXPRESS_TRANSACTION
,      LCM_SHIPMENT_LINE_ID
,      UNIT_LANDED_COST
,      LCM_ADJUSTMENT_NUM
 ,       KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.RCV_TRANSACTIONS_INTERFACE
	where kca_operation IN ('INSERT','UPDATE') 
	and (INTERFACE_TRANSACTION_ID,kca_seq_id) in 
	(select INTERFACE_TRANSACTION_ID,max(kca_seq_id) from bec_ods_stg.RCV_TRANSACTIONS_INTERFACE 
     where kca_operation IN ('INSERT','UPDATE')
     group by INTERFACE_TRANSACTION_ID)
);

commit;

-- Soft delete
update bec_ods.RCV_TRANSACTIONS_INTERFACE set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RCV_TRANSACTIONS_INTERFACE set IS_DELETED_FLG = 'Y'
where (INTERFACE_TRANSACTION_ID)  in
(
select INTERFACE_TRANSACTION_ID from bec_raw_dl_ext.RCV_TRANSACTIONS_INTERFACE
where (INTERFACE_TRANSACTION_ID,KCA_SEQ_ID)
in 
(
select INTERFACE_TRANSACTION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RCV_TRANSACTIONS_INTERFACE
group by INTERFACE_TRANSACTION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'RCV_TRANSACTIONS_INTERFACE'; 

commit;