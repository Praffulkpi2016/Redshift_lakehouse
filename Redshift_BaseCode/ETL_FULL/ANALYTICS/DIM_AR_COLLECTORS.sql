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
drop table if exists bec_dwh.DIM_AR_COLLECTORS;

CREATE TABLE  bec_dwh.DIM_AR_COLLECTORS 
	diststyle all sortkey(COLLECTOR_ID)
as
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
from BEC_ODS.AR_COLLECTORS
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'dim_ar_collectors'
	and batch_name = 'ar';

commit;