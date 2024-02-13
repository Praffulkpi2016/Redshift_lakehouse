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
truncate table bec_dwh.DIM_ASCP_HP_SOURCE_TYPES;

insert into bec_dwh.DIM_ASCP_HP_SOURCE_TYPES
( SELECT
     order_type_entity
	,'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                  AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    ||'-'|| nvl(order_type_entity, 'NA') AS dw_load_id,
    getdate()          AS dw_insert_date,
    getdate()          AS dw_update_date
FROM
    (
	select 'Gross Requirements' order_type_entity         
     union all
     select 'Total Supply'              
     union all
     select 'Current Scheduled Receipts' 
     union all
     select 'Projected Available Balance'
     union all
     select 'Projected On Hand'          
     union all
     select 'Sales Orders'               
     union all
     select 'Forecast'                   
     union all
     select 'Dependent Demand'           
     union all
     select 'Expected Scrap'             
     union all
     select 'Work Orders'                
     union all
     select 'Purchase order'             
     union all
     select 'Requisition'                
     union all
     select 'In transit'                 
     union all
     select 'In receiving'               
     union all
     select 'Planned order'              
     union all
     select 'On Hand'                    
     union all
     select 'Safety Stock' 
	 )
);

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ascp_hp_source_types' and batch_name = 'ascp';

COMMIT;

