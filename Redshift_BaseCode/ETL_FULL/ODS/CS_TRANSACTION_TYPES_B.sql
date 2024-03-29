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

DROP TABLE if exists bec_ods.CS_TRANSACTION_TYPES_B;

CREATE TABLE IF NOT EXISTS bec_ods.CS_TRANSACTION_TYPES_B
(
	TRANSACTION_TYPE_ID NUMERIC(15,0) ENCODE az64, 
	LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64, 
	CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	CREATED_BY NUMERIC(15,0) ENCODE az64, 
	LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64, 
	SEEDED_FLAG VARCHAR(1) ENCODE LZO, 
	REVISION_FLAG VARCHAR(1) ENCODE LZO, 
	END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
	INSTALLED_CP_STATUS_ID NUMERIC(15,0) ENCODE az64, 
	INSTALLED_STATUS_CODE VARCHAR(30) ENCODE LZO, 
	INSTALLED_CP_RETURN_REQUIRED VARCHAR(1) ENCODE LZO, 
	BILLING_FLAG VARCHAR(1) ENCODE LZO, 
	NEW_CP_STATUS_ID NUMERIC(15,0) ENCODE az64, 
	NEW_CP_STATUS_CODE VARCHAR(30) ENCODE LZO, 
	TRANSFER_SERVICE VARCHAR(1) ENCODE LZO, 
	NEW_CP_RETURN_REQUIRED VARCHAR(1) ENCODE LZO, 
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
	CONTEXT VARCHAR(30) ENCODE LZO, 
	NO_CHARGE_FLAG VARCHAR(1) ENCODE LZO, 
	MOVE_COUNTERS_FLAG VARCHAR(1) ENCODE LZO, 
	OBJECT_VERSION_NUMBER NUMERIC(15,0) ENCODE az64, 
	DEPOT_REPAIR_FLAG VARCHAR(1) ENCODE LZO, 
	SECURITY_GROUP_ID NUMERIC(15,0) ENCODE az64, 
	LINE_ORDER_CATEGORY_CODE VARCHAR(30) ENCODE LZO, 
	INTERFACE_TO_OE_FLAG VARCHAR(1) ENCODE LZO, 
	CREATE_COST_FLAG VARCHAR(3) ENCODE LZO, 
	CREATE_CHARGE_FLAG VARCHAR(3) ENCODE LZO, 
	TRAVEL_FLAG VARCHAR(1) ENCODE LZO, 
	CALCULATE_PRICE_FLAG VARCHAR(3) ENCODE LZO, 
	ZD_EDITION_NAME VARCHAR(30) ENCODE LZO, 
	ZD_SYNC VARCHAR(30) ENCODE LZO, 
	UPDATE_TASK_ACTUALS_FLAG VARCHAR(3) ENCODE LZO,
	KCA_OPERATION VARCHAR(10)   ENCODE lzo,
    IS_DELETED_FLG VARCHAR(2) ENCODE lzo,
	kca_seq_id NUMERIC(36,0)   ENCODE az64,
	kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

COMMIT;

INSERT INTO bec_ods.CS_TRANSACTION_TYPES_B (
	TRANSACTION_TYPE_ID, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATION_DATE, 
	CREATED_BY, 
	LAST_UPDATE_LOGIN, 
	SEEDED_FLAG, 
	REVISION_FLAG, 
	END_DATE_ACTIVE, 
	START_DATE_ACTIVE, 
	INSTALLED_CP_STATUS_ID, 
	INSTALLED_STATUS_CODE, 
	INSTALLED_CP_RETURN_REQUIRED, 
	BILLING_FLAG, 
	NEW_CP_STATUS_ID, 
	NEW_CP_STATUS_CODE, 
	TRANSFER_SERVICE, 
	NEW_CP_RETURN_REQUIRED, 
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
	CONTEXT, 
	NO_CHARGE_FLAG, 
	MOVE_COUNTERS_FLAG, 
	OBJECT_VERSION_NUMBER, 
	DEPOT_REPAIR_FLAG, 
	SECURITY_GROUP_ID, 
	LINE_ORDER_CATEGORY_CODE, 
	INTERFACE_TO_OE_FLAG, 
	CREATE_COST_FLAG, 
	CREATE_CHARGE_FLAG, 
	TRAVEL_FLAG, 
	CALCULATE_PRICE_FLAG, 
	ZD_EDITION_NAME, 
	ZD_SYNC, 
	UPDATE_TASK_ACTUALS_FLAG, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		TRANSACTION_TYPE_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		SEEDED_FLAG, 
		REVISION_FLAG, 
		END_DATE_ACTIVE, 
		START_DATE_ACTIVE, 
		INSTALLED_CP_STATUS_ID, 
		INSTALLED_STATUS_CODE, 
		INSTALLED_CP_RETURN_REQUIRED, 
		BILLING_FLAG, 
		NEW_CP_STATUS_ID, 
		NEW_CP_STATUS_CODE, 
		TRANSFER_SERVICE, 
		NEW_CP_RETURN_REQUIRED, 
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
		CONTEXT, 
		NO_CHARGE_FLAG, 
		MOVE_COUNTERS_FLAG, 
		OBJECT_VERSION_NUMBER, 
		DEPOT_REPAIR_FLAG, 
		SECURITY_GROUP_ID, 
		LINE_ORDER_CATEGORY_CODE, 
		INTERFACE_TO_OE_FLAG, 
		CREATE_COST_FLAG, 
		CREATE_CHARGE_FLAG, 
		TRAVEL_FLAG, 
		CALCULATE_PRICE_FLAG, 
		ZD_EDITION_NAME, 
		ZD_SYNC, 
		UPDATE_TASK_ACTUALS_FLAG, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.CS_TRANSACTION_TYPES_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cs_transaction_types_b';
	
commit;