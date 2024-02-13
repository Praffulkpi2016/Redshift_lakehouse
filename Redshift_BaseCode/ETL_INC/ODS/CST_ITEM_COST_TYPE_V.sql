/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach to ODS.
# File Version: KPI v1.0
*/


begin;

TRUNCATE TABLE bec_ods.CST_ITEM_COST_TYPE_V;

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