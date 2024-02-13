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
drop table if exists bec_dwh.DIM_INV_SAFTY_STOCK;

CREATE TABLE bec_dwh.DIM_INV_SAFTY_STOCK 
diststyle all 
sortkey(INVENTORY_ITEM_ID,ORGANIZATION_ID)
AS
(
SELECT 
      organization_id
	 ,inventory_item_id
	 --,plan_id
	 ,safety_stock_quantity
	 ,effectivity_date
	 ,safety_stock_code
	 ,'N' as is_deleted_flg,
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
    || '-'|| nvl(INVENTORY_ITEM_ID,0) || '-' || nvl(ORGANIZATION_ID, 0) || '-' || nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00')	 AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
	bec_ods.mtl_safety_stocks 
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_safty_stock' and batch_name = 'inv';

COMMIT;