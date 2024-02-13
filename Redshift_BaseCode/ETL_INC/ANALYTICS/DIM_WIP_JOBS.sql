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

delete from bec_dwh.DIM_WIP_JOBS
where (nvl(WIP_ENTITY_ID,0)) in (
select ods.WIP_ENTITY_ID from bec_dwh.DIM_WIP_JOBS dw, 
(select nvl(WIP_ENTITY_ID,0) WIP_ENTITY_ID from bec_ods.WIP_ENTITIES
where 1=1 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_wip_jobs' and batch_name = 'wip')
 
)
) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.WIP_ENTITY_ID,0) 
);

commit;

-- Insert records

INSERT INTO bec_dwh.DIM_WIP_JOBS
(
WIP_ENTITY_ID
,ORGANIZATION_ID 
,LAST_UPDATE_DATE 
,LAST_UPDATED_BY 
,CREATION_DATE 
,CREATED_BY 
,LAST_UPDATE_LOGIN 
,REQUEST_ID 
,PROGRAM_APPLICATION_ID 
,PROGRAM_ID 
,PROGRAM_UPDATE_DATE 
,WIP_ENTITY_NAME 
,ENTITY_TYPE 
,DESCRIPTION 
,PRIMARY_ITEM_ID 
,GEN_OBJECT_ID
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
    ( SELECT
        WIP_ENTITY_ID, 
		ORGANIZATION_ID, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATION_DATE, 
		CREATED_BY, 
		LAST_UPDATE_LOGIN, 
		REQUEST_ID, 
		PROGRAM_APPLICATION_ID, 
		PROGRAM_ID, 
		PROGRAM_UPDATE_DATE, 
		WIP_ENTITY_NAME, 
		ENTITY_TYPE, 
		DESCRIPTION, 
		PRIMARY_ITEM_ID, 
		GEN_OBJECT_ID,
		'N' as is_deleted_flg,
        (
            SELECT
                system_id
            FROM
                bec_etl_ctrl.etlsourceappid
            WHERE
                source_system = 'EBS'
        )                         AS source_app_id,
        (
            SELECT
                system_id
            FROM
                bec_etl_ctrl.etlsourceappid
            WHERE
                source_system = 'EBS'
        ) ||'-'|| nvl(WIP_ENTITY_ID, 0) AS dw_load_id,
        getdate()                 AS dw_insert_date,
        getdate()                 AS dw_update_date
    FROM
        bec_ods.WIP_ENTITIES
where 1=1 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_wip_jobs' and batch_name = 'wip')
 
)
    );

-- Soft delete

update bec_dwh.DIM_WIP_JOBS set is_deleted_flg = 'Y'
where (nvl(WIP_ENTITY_ID,0)) not in (
select nvl(ext.WIP_ENTITY_ID,0)  as WIP_ENTITY_ID from bec_dwh.DIM_WIP_JOBS dw, bec_ods.WIP_ENTITIES ext
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ext.WIP_ENTITY_ID,0) 
AND ext.is_deleted_flg <> 'Y');

commit;	

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
	load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_wip_jobs' and batch_name = 'wip';

COMMIT;