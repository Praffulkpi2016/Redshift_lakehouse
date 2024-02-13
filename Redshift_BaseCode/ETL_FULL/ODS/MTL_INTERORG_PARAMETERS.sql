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

DROP TABLE if exists bec_ods.mtl_interorg_parameters;

CREATE TABLE IF NOT EXISTS bec_ods.mtl_interorg_parameters
(
	FROM_ORGANIZATION_ID NUMERIC(15,0)   ENCODE az64
	,TO_ORGANIZATION_ID NUMERIC(15,0)  ENCODE az64
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,LAST_UPDATED_BY NUMERIC(15,0)   ENCODE az64
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CREATED_BY NUMERIC(15,0)   ENCODE az64
	,LAST_UPDATE_LOGIN NUMERIC(15,0)   ENCODE az64
	,INTRANSIT_TYPE NUMERIC(15,0)   ENCODE az64
	,DISTANCE_UOM_CODE VARCHAR(3) ENCODE lzo
	,TO_ORGANIZATION_DISTANCE NUMERIC(28,10)   ENCODE az64
	,FOB_POINT NUMERIC(15,0)   ENCODE az64
	,MATL_INTERORG_TRANSFER_CODE NUMERIC(15,0)   ENCODE az64
	,ROUTING_HEADER_ID NUMERIC(15,0)   ENCODE az64
	,INTERNAL_ORDER_REQUIRED_FLAG NUMERIC(15,0)   ENCODE az64
	,INTRANSIT_INV_ACCOUNT NUMERIC(15,0)   ENCODE az64
	,INTERORG_TRNSFR_CHARGE_PERCENT NUMERIC(28,10)   ENCODE az64
	,INTERORG_TRANSFER_CR_ACCOUNT NUMERIC(15,0)   ENCODE az64
	,INTERORG_RECEIVABLES_ACCOUNT NUMERIC(15,0)   ENCODE az64
	,INTERORG_PAYABLES_ACCOUNT NUMERIC(28,10)   ENCODE az64
	,INTERORG_PRICE_VAR_ACCOUNT NUMERIC(15,0)   ENCODE az64
	,ATTRIBUTE_CATEGORY VARCHAR(30)   ENCODE LZO
	,ATTRIBUTE1 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE2 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE3 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE4 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE5 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE6 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE7 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE8 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE9 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE10 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE11 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE12 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE13 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE14 VARCHAR(150)   ENCODE LZO
	,ATTRIBUTE15 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR(1)   ENCODE LZO
	,GLOBAL_ATTRIBUTE1 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE2 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE3 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE4 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE5 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE6 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE7 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE8 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE9 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE10 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE11 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE12 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE13 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE14 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE15 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE16 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE17 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE18 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE19 VARCHAR(150)   ENCODE LZO
	,GLOBAL_ATTRIBUTE20 VARCHAR(150)   ENCODE LZO
	,ELEMENTAL_VISIBILITY_ENABLED VARCHAR(1)   ENCODE LZO
	,MANUAL_RECEIPT_EXPENSE VARCHAR(1)   ENCODE LZO
	,PROFIT_IN_INV_ACCOUNT NUMERIC(15,0)   ENCODE az64
	,SHIKYU_ENABLED_FLAG VARCHAR(1)   ENCODE LZO
	,SHIKYU_DEFAULT_ORDER_TYPE_ID NUMERIC(15,0)  ENCODE az64
	,SHIKYU_OEM_VAR_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64
	,SHIKYU_TP_OFFSET_ACCOUNT_ID NUMERIC(15,0)   ENCODE az64
	,INTERORG_PROFIT_ACCOUNT NUMERIC(28,10)   ENCODE az64
	,PRICELIST_ID NUMERIC(15,0)   ENCODE az64
	,SUBCONTRACTING_TYPE VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.mtl_interorg_parameters (
	FROM_ORGANIZATION_ID,
	TO_ORGANIZATION_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	INTRANSIT_TYPE,
	DISTANCE_UOM_CODE,
	TO_ORGANIZATION_DISTANCE,
	FOB_POINT,
	MATL_INTERORG_TRANSFER_CODE,
	ROUTING_HEADER_ID,
	INTERNAL_ORDER_REQUIRED_FLAG,
	INTRANSIT_INV_ACCOUNT,
	INTERORG_TRNSFR_CHARGE_PERCENT,
	INTERORG_TRANSFER_CR_ACCOUNT,
	INTERORG_RECEIVABLES_ACCOUNT,
	INTERORG_PAYABLES_ACCOUNT,
	INTERORG_PRICE_VAR_ACCOUNT,
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
	GLOBAL_ATTRIBUTE_CATEGORY,
	GLOBAL_ATTRIBUTE1,
	GLOBAL_ATTRIBUTE2,
	GLOBAL_ATTRIBUTE3,
	GLOBAL_ATTRIBUTE4,
	GLOBAL_ATTRIBUTE5,
	GLOBAL_ATTRIBUTE6,
	GLOBAL_ATTRIBUTE7,
	GLOBAL_ATTRIBUTE8,
	GLOBAL_ATTRIBUTE9,
	GLOBAL_ATTRIBUTE10,
	GLOBAL_ATTRIBUTE11,
	GLOBAL_ATTRIBUTE12,
	GLOBAL_ATTRIBUTE13,
	GLOBAL_ATTRIBUTE14,
	GLOBAL_ATTRIBUTE15,
	GLOBAL_ATTRIBUTE16,
	GLOBAL_ATTRIBUTE17,
	GLOBAL_ATTRIBUTE18,
	GLOBAL_ATTRIBUTE19,
	GLOBAL_ATTRIBUTE20,
	ELEMENTAL_VISIBILITY_ENABLED,
	MANUAL_RECEIPT_EXPENSE,
	PROFIT_IN_INV_ACCOUNT,
	SHIKYU_ENABLED_FLAG,
	SHIKYU_DEFAULT_ORDER_TYPE_ID,
	SHIKYU_OEM_VAR_ACCOUNT_ID,
	SHIKYU_TP_OFFSET_ACCOUNT_ID,
	INTERORG_PROFIT_ACCOUNT,
	PRICELIST_ID,
	SUBCONTRACTING_TYPE,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
		FROM_ORGANIZATION_ID,
		TO_ORGANIZATION_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		INTRANSIT_TYPE,
		DISTANCE_UOM_CODE,
		TO_ORGANIZATION_DISTANCE,
		FOB_POINT,
		MATL_INTERORG_TRANSFER_CODE,
		ROUTING_HEADER_ID,
		INTERNAL_ORDER_REQUIRED_FLAG,
		INTRANSIT_INV_ACCOUNT,
		INTERORG_TRNSFR_CHARGE_PERCENT,
		INTERORG_TRANSFER_CR_ACCOUNT,
		INTERORG_RECEIVABLES_ACCOUNT,
		INTERORG_PAYABLES_ACCOUNT,
		INTERORG_PRICE_VAR_ACCOUNT,
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
		GLOBAL_ATTRIBUTE_CATEGORY,
		GLOBAL_ATTRIBUTE1,
		GLOBAL_ATTRIBUTE2,
		GLOBAL_ATTRIBUTE3,
		GLOBAL_ATTRIBUTE4,
		GLOBAL_ATTRIBUTE5,
		GLOBAL_ATTRIBUTE6,
		GLOBAL_ATTRIBUTE7,
		GLOBAL_ATTRIBUTE8,
		GLOBAL_ATTRIBUTE9,
		GLOBAL_ATTRIBUTE10,
		GLOBAL_ATTRIBUTE11,
		GLOBAL_ATTRIBUTE12,
		GLOBAL_ATTRIBUTE13,
		GLOBAL_ATTRIBUTE14,
		GLOBAL_ATTRIBUTE15,
		GLOBAL_ATTRIBUTE16,
		GLOBAL_ATTRIBUTE17,
		GLOBAL_ATTRIBUTE18,
		GLOBAL_ATTRIBUTE19,
		GLOBAL_ATTRIBUTE20,
		ELEMENTAL_VISIBILITY_ENABLED,
		MANUAL_RECEIPT_EXPENSE,
		PROFIT_IN_INV_ACCOUNT,
		SHIKYU_ENABLED_FLAG,
		SHIKYU_DEFAULT_ORDER_TYPE_ID,
		SHIKYU_OEM_VAR_ACCOUNT_ID,
		SHIKYU_TP_OFFSET_ACCOUNT_ID,
		INTERORG_PROFIT_ACCOUNT,
		PRICELIST_ID,
		SUBCONTRACTING_TYPE,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.mtl_interorg_parameters;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_interorg_parameters';
	
commit;