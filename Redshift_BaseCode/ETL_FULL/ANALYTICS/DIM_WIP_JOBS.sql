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
drop table if exists bec_dwh.DIM_WIP_JOBS;

CREATE TABLE bec_dwh.DIM_WIP_JOBS 
	diststyle all sortkey(WIP_ENTITY_ID)
AS
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
    )                    AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(WIP_ENTITY_ID, 0) AS dw_load_id,
    getdate()            AS dw_insert_date,
    getdate()            AS dw_update_date
FROM
    bec_ods.WIP_ENTITIES
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_wip_jobs' and batch_name = 'wip';

COMMIT;