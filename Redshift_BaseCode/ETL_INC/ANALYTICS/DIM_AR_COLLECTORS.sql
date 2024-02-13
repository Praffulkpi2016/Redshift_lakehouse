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

delete from bec_dwh.DIM_AR_COLLECTORS
where nvl(COLLECTOR_ID,0) in (
select nvl(ods.COLLECTOR_ID,0) from bec_dwh.DIM_AR_COLLECTORS dw, bec_ods.AR_COLLECTORS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.COLLECTOR_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_collectors' and batch_name = 'ar')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_AR_COLLECTORS
(
collector_id,
	"name",
	employee_id,
	description,
	status,
	inactive_date,
	alias,
	telephone_number,
	resource_id,
	resource_type,
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)

(
select
    COLLECTOR_ID,
    NAME,
	EMPLOYEE_ID,
	DESCRIPTION,
	STATUS,
	INACTIVE_DATE
	,ALIAS,
	TELEPHONE_NUMBER,	
	RESOURCE_ID,
	RESOURCE_TYPE,
	last_update_date,
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
       || nvl(COLLECTOR_ID,0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
 bec_ods.AR_COLLECTORS 
 where 1=1
 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_ar_collectors' and batch_name = 'ar')
 )
 );

-- Soft delete

update bec_dwh.DIM_AR_COLLECTORS set is_deleted_flg = 'Y'
where nvl(COLLECTOR_ID,0) not in (
select nvl(ods.COLLECTOR_ID,0) from bec_dwh.DIM_AR_COLLECTORS dw, bec_ods.AR_COLLECTORS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.COLLECTOR_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_collectors' and batch_name = 'ar';

COMMIT;