/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_dwh.DIM_BOM_RESOURCES;

CREATE TABLE  bec_dwh.DIM_BOM_RESOURCES
	diststyle all sortkey(RESOURCE_ID)
as
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
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_bom_resources'
	and batch_name = 'wip';

commit;