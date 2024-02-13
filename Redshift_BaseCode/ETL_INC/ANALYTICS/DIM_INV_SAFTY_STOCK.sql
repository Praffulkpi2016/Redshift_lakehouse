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
delete from bec_dwh.DIM_INV_SAFTY_STOCK
where (nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID, 0) ,
nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'))
in
(
select 
 nvl(ods.INVENTORY_ITEM_ID,0) INVENTORY_ITEM_ID, nvl(ods.ORGANIZATION_ID, 0)  ORGANIZATION_ID,
nvl(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') EFFECTIVITY_DATE
from bec_dwh.DIM_INV_SAFTY_STOCK dw,
(
SELECT 
      organization_id
	 ,inventory_item_id
	 ,effectivity_date
	FROM bec_ods.mtl_safety_stocks
	where kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
	where dw_table_name ='dim_inv_safty_stock' and batch_name = 'inv')
)ods
where 1=1
and dw.dw_load_id = 
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(ods.INVENTORY_ITEM_ID,0) || '-' || nvl(ods.ORGANIZATION_ID, 0)  
|| '-' || nvl(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') 
);

commit;

-- Insert Records
insert into bec_dwh.DIM_INV_SAFTY_STOCK
(
organization_id
,inventory_item_id
,safety_stock_quantity
,effectivity_date
,safety_stock_code
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
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
Where kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
	where dw_table_name ='dim_inv_safty_stock' and batch_name = 'inv')
);

-- Soft Delete

update bec_dwh.DIM_INV_SAFTY_STOCK set is_deleted_flg = 'Y'
where (nvl(INVENTORY_ITEM_ID,0),nvl(ORGANIZATION_ID, 0) ,
nvl(EFFECTIVITY_DATE, '1900-01-01 12:00:00'))
not in
(
select 
 nvl(ods.INVENTORY_ITEM_ID,0) INVENTORY_ITEM_ID, nvl(ods.ORGANIZATION_ID, 0)  ORGANIZATION_ID,
 nvl(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') EFFECTIVITY_DATE
from bec_dwh.DIM_INV_SAFTY_STOCK dw,
(SELECT 
      organization_id
	 ,inventory_item_id
	 ,effectivity_date
FROM
	(select * from bec_ods.mtl_safety_stocks where is_deleted_flg <> 'Y') 
)ods
where 1=1
and dw.dw_load_id = 
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(ods.INVENTORY_ITEM_ID,0) || '-' || nvl(ods.ORGANIZATION_ID, 0)  
|| '-' || nvl(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') 
);

end;
 

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_safty_stock' and batch_name = 'inv';

COMMIT;