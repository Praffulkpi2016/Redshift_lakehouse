/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach to ODS.
# File Version: KPI v1.0
*/


begin;

DROP TABLE if exists bec_ods.CST_ITEM_COST_TYPE_V;

CREATE TABLE IF NOT EXISTS bec_ods.CST_ITEM_COST_TYPE_V
(
	inventory_item_id NUMERIC(15,0)   ENCODE az64
	,item_number VARCHAR(40)   ENCODE lzo
	,padded_item_number VARCHAR(40)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,primary_uom_code VARCHAR(3)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,cost_type_id NUMERIC(15,0)   ENCODE az64
	,cost_type VARCHAR(10)   ENCODE lzo
	,cost_type_description VARCHAR(240)   ENCODE lzo
	,allow_updates_flag NUMERIC(28,10)   ENCODE az64
	,frozen_standard_flag NUMERIC(28,10)   ENCODE az64
	,default_cost_type_id NUMERIC(15,0)   ENCODE az64
	,default_cost_type VARCHAR(10)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(28,10)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(28,10)   ENCODE az64
	,last_update_login NUMERIC(28,10)   ENCODE az64
	,inventory_asset NUMERIC(28,10)   ENCODE az64
	,lot_size NUMERIC(28,10)   ENCODE az64
	,based_on_rollup NUMERIC(28,10)   ENCODE az64
	,shrinkage_rate NUMERIC(28,10)   ENCODE az64
	,defaulted_flag NUMERIC(28,10)   ENCODE az64
	,item_cost NUMERIC(28,10)   ENCODE az64
	,material_cost NUMERIC(28,10)   ENCODE az64
	,material_overhead_cost NUMERIC(28,10)   ENCODE az64
	,resource_cost NUMERIC(28,10)   ENCODE az64
	,outside_processing_cost NUMERIC(28,10)   ENCODE az64
	,overhead_cost NUMERIC(28,10)   ENCODE az64
	,planning_make_buy_code VARCHAR(80)   ENCODE lzo
	,default_include_in_rollup_flag VARCHAR(1)   ENCODE lzo
	,cost_of_sales_account NUMERIC(28,10)   ENCODE az64
	,sales_account NUMERIC(28,10)   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
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
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CST_ITEM_COST_TYPE_V (
	   INVENTORY_ITEM_ID
,      ITEM_NUMBER
,      PADDED_ITEM_NUMBER
,      DESCRIPTION
,      PRIMARY_UOM_CODE
,      ORGANIZATION_ID
,      COST_TYPE_ID
,      COST_TYPE
,      COST_TYPE_DESCRIPTION
,      ALLOW_UPDATES_FLAG
,      FROZEN_STANDARD_FLAG
,      DEFAULT_COST_TYPE_ID
,      DEFAULT_COST_TYPE
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      INVENTORY_ASSET
,      LOT_SIZE
,      BASED_ON_ROLLUP
,      SHRINKAGE_RATE
,      DEFAULTED_FLAG
,      ITEM_COST
,      MATERIAL_COST
,      MATERIAL_OVERHEAD_COST
,      RESOURCE_COST
,      OUTSIDE_PROCESSING_COST
,      OVERHEAD_COST
,      PLANNING_MAKE_BUY_CODE
,      DEFAULT_INCLUDE_IN_ROLLUP_FLAG
,      COST_OF_SALES_ACCOUNT
,      SALES_ACCOUNT
,      CATEGORY_ID
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
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,	   kca_operation
,	   is_deleted_flg
,	   kca_seq_id
,kca_seq_date
)
    SELECT
	   INVENTORY_ITEM_ID
,      ITEM_NUMBER
,      PADDED_ITEM_NUMBER
,      DESCRIPTION
,      PRIMARY_UOM_CODE
,      ORGANIZATION_ID
,      COST_TYPE_ID
,      COST_TYPE
,      COST_TYPE_DESCRIPTION
,      ALLOW_UPDATES_FLAG
,      FROZEN_STANDARD_FLAG
,      DEFAULT_COST_TYPE_ID
,      DEFAULT_COST_TYPE
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      INVENTORY_ASSET
,      LOT_SIZE
,      BASED_ON_ROLLUP
,      SHRINKAGE_RATE
,      DEFAULTED_FLAG
,      ITEM_COST
,      MATERIAL_COST
,      MATERIAL_OVERHEAD_COST
,      RESOURCE_COST
,      OUTSIDE_PROCESSING_COST
,      OVERHEAD_COST
,      PLANNING_MAKE_BUY_CODE
,      DEFAULT_INCLUDE_IN_ROLLUP_FLAG
,      COST_OF_SALES_ACCOUNT
,      SALES_ACCOUNT
,      CATEGORY_ID
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
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.CST_ITEM_COST_TYPE_V;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_item_cost_type_v';
	
commit;