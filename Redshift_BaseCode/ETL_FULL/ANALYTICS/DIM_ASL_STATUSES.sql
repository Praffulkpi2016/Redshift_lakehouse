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
drop table if exists bec_dwh.DIM_ASL_STATUSES;

CREATE TABLE bec_dwh.dim_asl_statuses 
diststyle all 
sortkey(status_id)
AS
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
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_asl_statuses' 
	and batch_name = 'po';

COMMIT;