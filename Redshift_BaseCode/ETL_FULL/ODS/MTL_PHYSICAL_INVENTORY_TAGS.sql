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

DROP TABLE if exists bec_ods.MTL_PHYSICAL_INVENTORY_TAGS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_PHYSICAL_INVENTORY_TAGS
(
	TAG_ID NUMERIC(15,0) ENCODE az64, 
	PHYSICAL_INVENTORY_ID NUMERIC(15,0) ENCODE az64, 
	ORGANIZATION_ID NUMERIC(15,0) ENCODE az64, 
	LAST_UPDATE_DATE  TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64, 
	CREATION_DATE  TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	CREATED_BY NUMERIC(15,0) ENCODE az64, 
	LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64, 
	VOID_FLAG NUMERIC(15,0) ENCODE az64, 
	TAG_NUMBER VARCHAR(40) ENCODE LZO, 
	ADJUSTMENT_ID NUMERIC(15,0) ENCODE az64, 
	INVENTORY_ITEM_ID NUMERIC(15,0) ENCODE az64, 
	TAG_QUANTITY NUMERIC(28,10) ENCODE az64, 
	TAG_UOM VARCHAR(3) ENCODE LZO, 
	TAG_QUANTITY_AT_STANDARD_UOM NUMERIC(28,10) ENCODE az64, 
	STANDARD_UOM VARCHAR(3) ENCODE LZO, 
	SUBINVENTORY VARCHAR(10) ENCODE LZO, 
	LOCATOR_ID NUMERIC(15,0) ENCODE az64, 
	LOT_NUMBER VARCHAR(80) ENCODE LZO, 
	LOT_EXPIRATION_DATE  TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	REVISION VARCHAR(3) ENCODE LZO, 
	SERIAL_NUM VARCHAR(30) ENCODE LZO, 
	COUNTED_BY_EMPLOYEE_ID NUMERIC(9,0) ENCODE az64, 
	LOT_SERIAL_CONTROLS VARCHAR(1) ENCODE LZO, 
	ATTRIBUTE_CATEGORY VARCHAR(30) ENCODE LZO, 
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
	REQUEST_ID NUMERIC(15,0) ENCODE az64, 
	PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64, 
	PROGRAM_ID NUMERIC(15,0) ENCODE az64, 
	PROGRAM_UPDATE_DATE  TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	PARENT_LPN_ID NUMERIC(15,0) ENCODE az64, 
	OUTERMOST_LPN_ID NUMERIC(15,0) ENCODE az64, 
	COST_GROUP_ID NUMERIC(15,0) ENCODE az64, 
	TAG_SECONDARY_QUANTITY NUMERIC(28,10) ENCODE az64, 
	TAG_SECONDARY_UOM VARCHAR(3) ENCODE LZO, 
	TAG_QTY_AT_STD_SECONDARY_UOM NUMERIC(28,10) ENCODE az64, 
	STANDARD_SECONDARY_UOM VARCHAR(3) ENCODE LZO, 
	KCA_OPERATION VARCHAR(10)   ENCODE lzo,
    IS_DELETED_FLG VARCHAR(2) ENCODE lzo,
	kca_seq_id NUMERIC(36,0)   ENCODE az64,
	kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

COMMIT;

INSERT INTO bec_ods.MTL_PHYSICAL_INVENTORY_TAGS (
	TAG_ID, 
	PHYSICAL_INVENTORY_ID, 
	ORGANIZATION_ID, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	VOID_FLAG, 
	TAG_NUMBER, 
	ADJUSTMENT_ID, 
	INVENTORY_ITEM_ID, 
	TAG_QUANTITY, 
	TAG_UOM, 
	TAG_QUANTITY_AT_STANDARD_UOM, 
	STANDARD_UOM, 
	SUBINVENTORY, 
	LOCATOR_ID, 
	LOT_NUMBER, 
	LOT_EXPIRATION_DATE, 
	REVISION, 
	SERIAL_NUM, 
	COUNTED_BY_EMPLOYEE_ID, 
	LOT_SERIAL_CONTROLS, 
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
	REQUEST_ID, 
	PROGRAM_APPLICATION_ID, 
	PROGRAM_ID, 
	PROGRAM_UPDATE_DATE, 
	PARENT_LPN_ID, 
	OUTERMOST_LPN_ID, 
	COST_GROUP_ID, 
	TAG_SECONDARY_QUANTITY, 
	TAG_SECONDARY_UOM, 
	TAG_QTY_AT_STD_SECONDARY_UOM, 
	STANDARD_SECONDARY_UOM,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		TAG_ID, 
		PHYSICAL_INVENTORY_ID, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		VOID_FLAG, 
		TAG_NUMBER, 
		ADJUSTMENT_ID, 
		INVENTORY_ITEM_ID, 
		TAG_QUANTITY, 
		TAG_UOM, 
		TAG_QUANTITY_AT_STANDARD_UOM, 
		STANDARD_UOM, 
		SUBINVENTORY, 
		LOCATOR_ID, 
		LOT_NUMBER, 
		LOT_EXPIRATION_DATE, 
		REVISION, 
		SERIAL_NUM, 
		COUNTED_BY_EMPLOYEE_ID, 
		LOT_SERIAL_CONTROLS, 
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
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		PARENT_LPN_ID, 
		OUTERMOST_LPN_ID, 
		COST_GROUP_ID, 
		TAG_SECONDARY_QUANTITY, 
		TAG_SECONDARY_UOM, 
		TAG_QTY_AT_STD_SECONDARY_UOM, 
		STANDARD_SECONDARY_UOM,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_physical_inventory_tags';
	
commit;