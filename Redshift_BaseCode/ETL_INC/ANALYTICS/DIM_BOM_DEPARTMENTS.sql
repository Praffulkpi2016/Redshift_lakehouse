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
delete from bec_dwh.dim_bom_departments
where (nvl(DEPARTMENT_ID,0)) in
(
select ods.DEPARTMENT_ID from bec_dwh.dim_bom_departments dw,
(select nvl(DEPARTMENT_ID,0) DEPARTMENT_ID from bec_ods.BOM_DEPARTMENTS
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_bom_departments' 
and batch_name = 'wip') )
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
       || nvl(ods.DEPARTMENT_ID,0)
);

-- Insert Records
INSERT INTO bec_dwh.dim_bom_departments
(
	DEPARTMENT_ID,
	DEPARTMENT_CODE,
	ORGANIZATION_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	DESCRIPTION,
	DISABLE_DATE,
	DEPARTMENT_CLASS_CODE,
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
	LOCATION_ID,
	PA_EXPENDITURE_ORG_ID,
	SCRAP_ACCOUNT,
	EST_ABSORPTION_ACCOUNT,
	MAINT_COST_CATEGORY,
--Audit COLUMNS
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
SELECT
	DEPARTMENT_ID,
	DEPARTMENT_CODE,
	ORGANIZATION_ID,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	DESCRIPTION,
	DISABLE_DATE,
	DEPARTMENT_CLASS_CODE,
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
	LOCATION_ID,
	PA_EXPENDITURE_ORG_ID,
	SCRAP_ACCOUNT,
	EST_ABSORPTION_ACCOUNT,
	MAINT_COST_CATEGORY,
--Audit COLUMNS
	'N' as is_deleted_flg,
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
       || nvl(DEPARTMENT_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM BEC_ODS.BOM_DEPARTMENTS
where 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_bom_departments' 
and batch_name = 'wip')
 )
);


-- Soft Delete
update bec_dwh.dim_bom_departments set is_deleted_flg = 'Y'
where (nvl(DEPARTMENT_ID,0)) not in 
(
select nvl(ods.DEPARTMENT_ID,0) as DEPARTMENT_ID from bec_dwh.dim_bom_departments dw, bec_ods.BOM_DEPARTMENTS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.DEPARTMENT_ID,0) 
AND ods.is_deleted_flg <> 'Y'
);

commit;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_bom_departments'
	and batch_name = 'wip';

commit;