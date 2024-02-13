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

DROP TABLE if exists bec_ods.RCV_TRANSACTIONS_INTERFACE;

CREATE TABLE IF NOT EXISTS bec_ods.RCV_TRANSACTIONS_INTERFACE
(
		 interface_transaction_id NUMERIC(15,0)   ENCODE az64
	,group_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transaction_type VARCHAR(25)   ENCODE lzo
	,transaction_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,processing_status_code VARCHAR(25)   ENCODE lzo
	,processing_mode_code VARCHAR(25)   ENCODE lzo
	,processing_request_id NUMERIC(15,0)   ENCODE az64
	,transaction_status_code VARCHAR(25)   ENCODE lzo
	,category_id NUMERIC(15,0)   ENCODE az64
	,quantity NUMERIC(28,10)   ENCODE az64
	,unit_of_measure VARCHAR(25)   ENCODE lzo
	,interface_source_code VARCHAR(30)   ENCODE lzo
	,interface_source_line_id NUMERIC(15,0)   ENCODE az64
	,inv_transaction_id NUMERIC(15,0)   ENCODE az64
	,item_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(240)   ENCODE lzo
	,item_revision VARCHAR(3)   ENCODE lzo
	,uom_code VARCHAR(3)   ENCODE lzo
	,employee_id INTEGER   ENCODE az64
	,auto_transact_code VARCHAR(25)   ENCODE lzo
	,shipment_header_id NUMERIC(15,0)   ENCODE az64
	,shipment_line_id NUMERIC(15,0)   ENCODE az64
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,primary_quantity NUMERIC(28,10)   ENCODE az64
	,primary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,receipt_source_code VARCHAR(25)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,from_organization_id NUMERIC(15,0)   ENCODE az64
	,from_subinventory VARCHAR(10)   ENCODE lzo
	,to_organization_id NUMERIC(15,0)   ENCODE az64
	,intransit_owning_org_id NUMERIC(15,0)   ENCODE az64
	,routing_header_id NUMERIC(15,0)   ENCODE az64
	,routing_step_id NUMERIC(15,0)   ENCODE az64
	,source_document_code VARCHAR(25)   ENCODE lzo
	,parent_transaction_id NUMERIC(15,0)   ENCODE az64
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,po_revision_num NUMERIC(15,0)   ENCODE az64
	,po_release_id NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,po_line_location_id NUMERIC(15,0)   ENCODE az64
	,po_unit_price NUMERIC(28,10)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,currency_conversion_type VARCHAR(30)   ENCODE lzo
	,currency_conversion_rate NUMERIC(28,10)   ENCODE az64
	,currency_conversion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,po_distribution_id NUMERIC(15,0)   ENCODE az64
	,requisition_line_id NUMERIC(15,0)   ENCODE az64
	,req_distribution_id NUMERIC(15,0)   ENCODE az64
	,charge_account_id NUMERIC(15,0)   ENCODE az64
	,substitute_unordered_code VARCHAR(25)   ENCODE lzo
	,receipt_exception_flag VARCHAR(1)   ENCODE lzo
	,accrual_status_code VARCHAR(25)   ENCODE lzo
	,inspection_status_code VARCHAR(25)   ENCODE lzo
	,inspection_quality_code VARCHAR(25)   ENCODE lzo
	,destination_type_code VARCHAR(25)   ENCODE lzo
	,deliver_to_person_id NUMERIC(15,0)   ENCODE az64
	,location_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_location_id NUMERIC(15,0)   ENCODE az64
	,subinventory VARCHAR(10)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,wip_entity_id NUMERIC(15,0)   ENCODE az64
	,wip_line_id NUMERIC(15,0)   ENCODE az64
	,department_code VARCHAR(10)   ENCODE lzo
	,wip_repetitive_schedule_id NUMERIC(15,0)   ENCODE az64
	,wip_operation_seq_num NUMERIC(15,0)   ENCODE az64
	,wip_resource_seq_num NUMERIC(15,0)   ENCODE az64
	,bom_resource_id NUMERIC(15,0)   ENCODE az64
	,shipment_num VARCHAR(30)   ENCODE lzo
	,freight_carrier_code VARCHAR(25)   ENCODE lzo
	,bill_of_lading VARCHAR(25)   ENCODE lzo
	,packing_slip VARCHAR(25)   ENCODE lzo
	,shipped_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,expected_receipt_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_cost NUMERIC(28,10)   ENCODE az64
	,transfer_cost NUMERIC(28,10)   ENCODE az64
	,transportation_cost NUMERIC(28,10)   ENCODE az64
	,transportation_account_id NUMERIC(15,0)   ENCODE az64
	,num_of_containers NUMERIC(15,0)   ENCODE az64
	,waybill_airbill_num VARCHAR(30)   ENCODE lzo
	,vendor_item_num VARCHAR(25)   ENCODE lzo
	,vendor_lot_num VARCHAR(30)   ENCODE lzo
	,rma_reference VARCHAR(30)   ENCODE lzo
	,comments VARCHAR(240)   ENCODE lzo
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute_category VARCHAR(30)   ENCODE lzo
	,ship_head_attribute1 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute2 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute3 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute4 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute5 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute6 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute7 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute8 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute9 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute10 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute11 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute12 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute13 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute14 VARCHAR(150)   ENCODE lzo
	,ship_head_attribute15 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute_category VARCHAR(30)   ENCODE lzo
	,ship_line_attribute1 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute2 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute3 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute4 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute5 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute6 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute7 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute8 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute9 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute10 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute11 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute12 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute13 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute14 VARCHAR(150)   ENCODE lzo
	,ship_line_attribute15 VARCHAR(150)   ENCODE lzo
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,government_context VARCHAR(30)   ENCODE lzo
	,reason_id NUMERIC(15,0)   ENCODE az64
	,destination_context VARCHAR(30)   ENCODE lzo
	,source_doc_quantity NUMERIC(28,10)   ENCODE az64
	,source_doc_unit_of_measure VARCHAR(25)   ENCODE lzo
	,movement_id NUMERIC(15,0)   ENCODE az64
	,header_interface_id NUMERIC(15,0)   ENCODE az64
	,vendor_cum_shipped_qty NUMERIC(28,10)   ENCODE az64
	,item_num VARCHAR(81)   ENCODE lzo
	,document_num VARCHAR(50)   ENCODE lzo
	,document_line_num NUMERIC(15,0)   ENCODE az64
	,truck_num VARCHAR(35)   ENCODE lzo
	,ship_to_location_code VARCHAR(60)   ENCODE lzo
	,container_num VARCHAR(35)   ENCODE lzo
	,substitute_item_num VARCHAR(81)   ENCODE lzo
	,notice_unit_price NUMERIC(28,10)   ENCODE az64
	,item_category VARCHAR(81)   ENCODE lzo
	,location_code VARCHAR(60)   ENCODE lzo
	,vendor_name VARCHAR(240)   ENCODE lzo
	,vendor_num VARCHAR(30)   ENCODE lzo
	,vendor_site_code VARCHAR(15)   ENCODE lzo
	,from_organization_code VARCHAR(3)   ENCODE lzo
	,to_organization_code VARCHAR(3)   ENCODE lzo
	,intransit_owning_org_code VARCHAR(3)   ENCODE lzo
	,routing_code VARCHAR(30)   ENCODE lzo
	,routing_step VARCHAR(30)   ENCODE lzo
	,release_num NUMERIC(15,0)   ENCODE az64
	,document_shipment_line_num NUMERIC(15,0)   ENCODE az64
	,document_distribution_num NUMERIC(15,0)   ENCODE az64
	,deliver_to_person_name VARCHAR(240)   ENCODE lzo
	,deliver_to_location_code VARCHAR(60)   ENCODE lzo
	,use_mtl_lot NUMERIC(15,0)   ENCODE az64
	,use_mtl_serial NUMERIC(15,0)   ENCODE az64
	,locator VARCHAR(81)   ENCODE lzo
	,reason_name VARCHAR(30)   ENCODE lzo
	,validation_flag VARCHAR(1)   ENCODE lzo
	,substitute_item_id NUMERIC(15,0)   ENCODE az64
	,quantity_shipped NUMERIC(28,10)   ENCODE az64
	,quantity_invoiced NUMERIC(28,10)   ENCODE az64
	,tax_name VARCHAR(50)   ENCODE lzo
	,tax_amount NUMERIC(28,10)   ENCODE az64
	,req_num VARCHAR(25)   ENCODE lzo
	,req_line_num NUMERIC(15,0)   ENCODE az64
	,req_distribution_num NUMERIC(15,0)   ENCODE az64
	,wip_entity_name VARCHAR(24)   ENCODE lzo
	,wip_line_code VARCHAR(10)   ENCODE lzo
	,resource_code VARCHAR(30)   ENCODE lzo
	,shipment_line_status_code VARCHAR(25)   ENCODE lzo
	,barcode_label VARCHAR(35)   ENCODE lzo
	,transfer_percentage NUMERIC(28,10)   ENCODE az64
	,qa_collection_id NUMERIC(15,0)   ENCODE az64
	,country_of_origin_code VARCHAR(2)   ENCODE lzo
	,oe_order_header_id NUMERIC(15,0)   ENCODE az64
	,oe_order_line_id NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,customer_site_id NUMERIC(15,0)   ENCODE az64
	,customer_item_num VARCHAR(50)   ENCODE lzo
	,create_debit_memo_flag VARCHAR(1)   ENCODE lzo
	,put_away_rule_id NUMERIC(15,0)   ENCODE az64
	,put_away_strategy_id NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,transfer_lpn_id NUMERIC(15,0)   ENCODE az64
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,mobile_txn VARCHAR(2)   ENCODE lzo
	,mmtt_temp_id NUMERIC(15,0)   ENCODE az64
	,transfer_cost_group_id NUMERIC(15,0)   ENCODE az64
	,secondary_quantity NUMERIC(28,10)   ENCODE az64
	,secondary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,secondary_uom_code VARCHAR(3)   ENCODE lzo
	,qc_grade VARCHAR(150)   ENCODE lzo
	,from_locator VARCHAR(81)   ENCODE lzo
	,from_locator_id NUMERIC(15,0)   ENCODE az64
	,parent_source_transaction_num VARCHAR(25)   ENCODE lzo
	,interface_available_qty NUMERIC(28,10)   ENCODE az64
	,interface_transaction_qty NUMERIC(28,10)   ENCODE az64
	,interface_available_amt NUMERIC(28,10)   ENCODE az64
	,interface_transaction_amt NUMERIC(28,10)   ENCODE az64
	,license_plate_number VARCHAR(30)   ENCODE lzo
	,source_transaction_num VARCHAR(150)   ENCODE lzo
	,transfer_license_plate_number VARCHAR(30)   ENCODE lzo
	,lpn_group_id NUMERIC(15,0)   ENCODE az64
	,order_transaction_id NUMERIC(15,0)   ENCODE az64
	,customer_account_number NUMERIC(15,0)   ENCODE az64
	,customer_party_name VARCHAR(360)   ENCODE lzo
	,oe_order_line_num NUMERIC(15,0)   ENCODE az64
	,oe_order_num NUMERIC(15,0)   ENCODE az64
	,parent_interface_txn_id NUMERIC(15,0)   ENCODE az64
	,customer_item_id NUMERIC(15,0)   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,job_id NUMERIC(15,0)   ENCODE az64
	,timecard_id NUMERIC(15,0)   ENCODE az64
	,timecard_ovn NUMERIC(15,0)   ENCODE az64
	,erecord_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,asn_attach_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,operating_unit VARCHAR(240)   ENCODE lzo
	,requested_amount NUMERIC(28,10)   ENCODE az64
	,material_stored_amount NUMERIC(28,10)   ENCODE az64
	,amount_shipped NUMERIC(28,10)   ENCODE az64
	,matching_basis VARCHAR(30)   ENCODE lzo
	,replenish_order_line_id NUMERIC(15,0)   ENCODE az64
	,express_transaction VARCHAR(1)   ENCODE lzo
	,lcm_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,unit_landed_cost NUMERIC(28,10)   ENCODE az64
	,lcm_adjustment_num NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RCV_TRANSACTIONS_INTERFACE (
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
,	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.RCV_TRANSACTIONS_INTERFACE;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'rcv_transactions_interface';
	
commit;