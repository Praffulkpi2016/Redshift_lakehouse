/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records
delete from bec_dwh.DIM_BOM_RESOURCES
where (nvl(RESOURCE_ID,0)) in
(
select ods.RESOURCE_ID from bec_dwh.DIM_BOM_RESOURCES dw,
(select nvl(RESOURCE_ID,0) RESOURCE_ID from bec_ods.BOM_RESOURCES
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_bom_resources' 
and batch_name = 'wip')
 )
) ods
where dw.dw_load_id = 
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(ods.RESOURCE_ID,0)
);

-- Insert Records
INSERT INTO bec_dwh.DIM_BOM_RESOURCES
(
RESOURCE_ID
,RESOURCE_CODE
,ORGANIZATION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,DISABLE_DATE
,COST_ELEMENT_ID
,PURCHASE_ITEM_ID
,COST_CODE_TYPE
,FUNCTIONAL_CURRENCY_FLAG
,UNIT_OF_MEASURE
,DEFAULT_ACTIVITY_ID
,RESOURCE_TYPE
,AUTOCHARGE_TYPE
,STANDARD_RATE_FLAG
,DEFAULT_BASIS_TYPE
,ABSORPTION_ACCOUNT
,ALLOW_COSTS_FLAG
,RATE_VARIANCE_ACCOUNT
,EXPENDITURE_TYPE
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
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,BATCHABLE
,MAX_BATCH_CAPACITY
,MIN_BATCH_CAPACITY
,BATCH_CAPACITY_UOM
,BATCH_WINDOW
,BATCH_WINDOW_UOM
,COMPETENCE_ID
,RATING_LEVEL_ID
,QUALIFICATION_TYPE_ID
,BILLABLE_ITEM_ID
,SUPPLY_SUBINVENTORY
,SUPPLY_LOCATOR_ID
,BATCHING_PENALTY
--Audit COLUMNS
,is_deleted_flg,
source_app_id,
dw_load_id,
dw_insert_date,
dw_update_date
)
(
SELECT
RESOURCE_ID
,RESOURCE_CODE
,ORGANIZATION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,DESCRIPTION
,DISABLE_DATE
,COST_ELEMENT_ID
,PURCHASE_ITEM_ID
,COST_CODE_TYPE
,FUNCTIONAL_CURRENCY_FLAG
,UNIT_OF_MEASURE
,DEFAULT_ACTIVITY_ID
,RESOURCE_TYPE
,AUTOCHARGE_TYPE
,STANDARD_RATE_FLAG
,DEFAULT_BASIS_TYPE
,ABSORPTION_ACCOUNT
,ALLOW_COSTS_FLAG
,RATE_VARIANCE_ACCOUNT
,EXPENDITURE_TYPE
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
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,REQUEST_ID
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,PROGRAM_UPDATE_DATE
,BATCHABLE
,MAX_BATCH_CAPACITY
,MIN_BATCH_CAPACITY
,BATCH_CAPACITY_UOM
,BATCH_WINDOW
,BATCH_WINDOW_UOM
,COMPETENCE_ID
,RATING_LEVEL_ID
,QUALIFICATION_TYPE_ID
,BILLABLE_ITEM_ID
,SUPPLY_SUBINVENTORY
,SUPPLY_LOCATOR_ID
,BATCHING_PENALTY
--Audit COLUMNS
,	'N' as is_deleted_flg,
 (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(RESOURCE_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM BEC_ODS.BOM_RESOURCES
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_bom_resources' 
and batch_name = 'wip')
 )
);


-- Soft Delete
update bec_dwh.dim_bom_resources set is_deleted_flg = 'Y'
where (nvl(RESOURCE_ID,0)) not in 
(
select nvl(ods.RESOURCE_ID,0) as RESOURCE_ID from bec_dwh.dim_bom_resources dw, bec_ods.bom_resources ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.RESOURCE_ID,0) 
AND ods.is_deleted_flg <> 'Y'
);

commit;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_bom_resources'
	and batch_name = 'wip';

commit;