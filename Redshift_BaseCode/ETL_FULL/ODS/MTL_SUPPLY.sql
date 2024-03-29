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

DROP TABLE if exists bec_ods.mtl_supply;

CREATE TABLE IF NOT EXISTS bec_ods.mtl_supply
(
	SUPPLY_TYPE_CODE VARCHAR(25)   ENCODE lzo
	,SUPPLY_SOURCE_ID NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
	,CREATED_BY NUMERIC(15,0)   ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,REQUEST_ID NUMERIC(15,0)   ENCODE az64
	,PROGRAM_APPLICATION_ID NUMERIC(15,0)   ENCODE az64
	,PROGRAM_ID NUMERIC(15,0)   ENCODE az64
	,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,REQ_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,REQ_LINE_ID NUMERIC(15,0)   ENCODE az64
	,PO_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,PO_RELEASE_ID NUMERIC(15,0)   ENCODE az64
	,PO_LINE_ID NUMERIC(15,0)   ENCODE az64
	,PO_LINE_LOCATION_ID NUMERIC(15,0)   ENCODE az64
	,PO_DISTRIBUTION_ID NUMERIC(15,0)   ENCODE az64
	,SHIPMENT_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,SHIPMENT_LINE_ID NUMERIC(15,0)   ENCODE az64
	,RCV_TRANSACTION_ID NUMERIC(15,0)   ENCODE az64
	,ITEM_ID NUMERIC(15,0)   ENCODE az64
	,ITEM_REVISION VARCHAR(3)   ENCODE lzo
	,CATEGORY_ID NUMERIC(15,0)   ENCODE az64
	,QUANTITY NUMERIC(28,10)   ENCODE az64	
	,UNIT_OF_MEASURE VARCHAR(25)   ENCODE lzo
	,TO_ORG_PRIMARY_QUANTITY NUMERIC(28,10)   ENCODE az64
	,TO_ORG_PRIMARY_UOM VARCHAR(25)   ENCODE LZO
	,RECEIPT_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,NEED_BY_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,EXPECTED_DELIVERY_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,DESTINATION_TYPE_CODE VARCHAR(25)   ENCODE LZO
	,LOCATION_ID NUMERIC(15,0)   ENCODE az64
	,FROM_ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64
	,FROM_SUBINVENTORY VARCHAR(10)   ENCODE LZO
	,TO_ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64
	,TO_SUBINVENTORY VARCHAR(10)   ENCODE LZO
	,INTRANSIT_OWNING_ORG_ID NUMERIC(15,0)   ENCODE az64
	,MRP_PRIMARY_QUANTITY NUMERIC(28,10)   ENCODE az64	
	,MRP_PRIMARY_UOM VARCHAR(25)   ENCODE LZO
	,MRP_EXPECTED_DELIVERY_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64		
	,MRP_DESTINATION_TYPE_CODE VARCHAR(150)   ENCODE LZO
	,MRP_TO_ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64	
	,MRP_TO_SUBINVENTORY VARCHAR(10)   ENCODE lzo	
	,CHANGE_FLAG VARCHAR(1)   ENCODE lzo
	,CHANGE_TYPE VARCHAR(25)   ENCODE lzo
	,COST_GROUP_ID NUMERIC(15,0)   ENCODE az64
	,EXCLUDE_FROM_PLANNING VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.mtl_supply (
	SUPPLY_TYPE_CODE,
	SUPPLY_SOURCE_ID,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATE_LOGIN,
	CREATED_BY,
	CREATION_DATE,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	REQ_HEADER_ID,
	REQ_LINE_ID,
	PO_HEADER_ID,
	PO_RELEASE_ID,
	PO_LINE_ID,
	PO_LINE_LOCATION_ID,
	PO_DISTRIBUTION_ID,
	SHIPMENT_HEADER_ID,
	SHIPMENT_LINE_ID,
	RCV_TRANSACTION_ID,
	ITEM_ID,
	ITEM_REVISION,
	CATEGORY_ID,
	QUANTITY,
	UNIT_OF_MEASURE,
	TO_ORG_PRIMARY_QUANTITY,
	TO_ORG_PRIMARY_UOM,
	RECEIPT_DATE,
	NEED_BY_DATE,
	EXPECTED_DELIVERY_DATE,
	DESTINATION_TYPE_CODE,
	LOCATION_ID,
	FROM_ORGANIZATION_ID,
	FROM_SUBINVENTORY,
	TO_ORGANIZATION_ID,
	TO_SUBINVENTORY,
	INTRANSIT_OWNING_ORG_ID,
	MRP_PRIMARY_QUANTITY,
	MRP_PRIMARY_UOM,
	MRP_EXPECTED_DELIVERY_DATE,
	MRP_DESTINATION_TYPE_CODE,
	MRP_TO_ORGANIZATION_ID,
	MRP_TO_SUBINVENTORY,
	CHANGE_FLAG,
	CHANGE_TYPE,
	COST_GROUP_ID,
	EXCLUDE_FROM_PLANNING,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
		SUPPLY_TYPE_CODE,
		SUPPLY_SOURCE_ID,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATE_LOGIN,
		CREATED_BY,
		CREATION_DATE,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
		REQ_HEADER_ID,
		REQ_LINE_ID,
		PO_HEADER_ID,
		PO_RELEASE_ID,
		PO_LINE_ID,
		PO_LINE_LOCATION_ID,
		PO_DISTRIBUTION_ID,
		SHIPMENT_HEADER_ID,
		SHIPMENT_LINE_ID,
		RCV_TRANSACTION_ID,
		ITEM_ID,
		ITEM_REVISION,
		CATEGORY_ID,
		QUANTITY,
		UNIT_OF_MEASURE,
		TO_ORG_PRIMARY_QUANTITY,
		TO_ORG_PRIMARY_UOM,
		RECEIPT_DATE,
		NEED_BY_DATE,
		EXPECTED_DELIVERY_DATE,
		DESTINATION_TYPE_CODE,
		LOCATION_ID,
		FROM_ORGANIZATION_ID,
		FROM_SUBINVENTORY,
		TO_ORGANIZATION_ID,
		TO_SUBINVENTORY,
		INTRANSIT_OWNING_ORG_ID,
		MRP_PRIMARY_QUANTITY,
		MRP_PRIMARY_UOM,
		MRP_EXPECTED_DELIVERY_DATE,
		MRP_DESTINATION_TYPE_CODE,
		MRP_TO_ORGANIZATION_ID,
		MRP_TO_SUBINVENTORY,
		CHANGE_FLAG,
		CHANGE_TYPE,
		COST_GROUP_ID,
		EXCLUDE_FROM_PLANNING,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.mtl_supply;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_supply';
	
commit;