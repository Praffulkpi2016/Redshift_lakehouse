/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records

delete from bec_dwh.DIM_ASL_STATUSES
where (nvl(status_id, 0)) in (
select nvl(ods.status_id, 0) as status_id from bec_dwh.dim_asl_statuses dw,
(SELECT status_id 
 FROM bec_ods.po_asl_statuses 
 WHERE kca_seq_date > (select (executebegints-prune_days) 
 from bec_etl_ctrl.batch_dw_info where 
 dw_table_name ='dim_asl_statuses' and batch_name = 'po')) ods
where dw.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')|| '-'|| nvl(ods.status_id, 0)
);

commit;

-- Insert records

insert into bec_dwh.dim_asl_statuses
(
	status_id,
    status,
    STATUS_DESCRIPTION, 
    ASL_DEFAULT_FLAG,
    creation_date,
    CREATED_BY, 
    LAST_UPDATE_DATE, 
    LAST_UPDATED_BY, 
    LAST_UPDATE_LOGIN,
	is_deleted_flg,
	SOURCE_APP_ID,
	DW_LOAD_ID,
	DW_INSERT_DATE,
	DW_UPDATE_DATE
)
(
SELECT 
	status_id
   ,status
   ,STATUS_DESCRIPTION 
   ,ASL_DEFAULT_FLAG
   ,creation_date
   ,CREATED_BY 
   ,LAST_UPDATE_DATE 
   ,LAST_UPDATED_BY 
   ,LAST_UPDATE_LOGIN,
	 'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )           AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(status_id,0) 
	 AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
	bec_ods.po_asl_statuses 
 where 
 kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
 where dw_table_name ='dim_asl_statuses' and batch_name = 'po')
 );
 
 -- Soft delete

update bec_dwh.dim_asl_statuses set is_deleted_flg = 'Y'
where (nvl(status_id, 0)) not in (
select nvl(ods.status_id, 0) as status_id 
from bec_dwh.dim_asl_statuses dw, (select * from bec_ods.po_asl_statuses ) ods
where dw.dw_load_id = (SELECT system_id FROM bec_etl_ctrl.etlsourceappid WHERE source_system = 'EBS')|| '-'|| nvl(ods.status_id, 0)
AND ods.is_deleted_flg <> 'Y'
);

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_asl_statuses'
and batch_name = 'po';

COMMIT;