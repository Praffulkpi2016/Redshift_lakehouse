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
drop table if exists bec_dwh.DIM_MSC_SAFTY_STOCK;

CREATE TABLE bec_dwh.DIM_MSC_SAFTY_STOCK 
diststyle all 
sortkey(INVENTORY_ITEM_ID,ORGANIZATION_ID)
AS
(
SELECT 
      m.organization_id
	 ,m.inventory_item_id
	 ,s.plan_id
	 ,s.safety_stock_quantity
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
    || '-'|| nvl(m.INVENTORY_ITEM_ID,0) || '-' || nvl(m.ORGANIZATION_ID, 0)  
|| '-' || nvl(s.PLAN_ID, 0) 	
	 AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
	bec_ods.MSC_SAFETY_STOCKS s,
	bec_ods.MSC_ITEMS i,
	bec_ods.mtl_system_items_b m
WHERE
	m.segment1 = i.item_name
	AND m.organization_id = s.organization_id
	AND s.inventory_item_id = i.inventory_item_id
	AND s.period_start_date =
                                   (
	SELECT
		MIN (Period_start_date)
	FROM
		bec_ods.MSC_SAFETY_STOCKS
	WHERE
		organization_id =s.organization_id
		AND inventory_item_id = s.inventory_item_id
		AND plan_id = s.plan_id)
);
	
end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_msc_safty_stock' and batch_name = 'ascp';

COMMIT;