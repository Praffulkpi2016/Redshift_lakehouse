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
--delete statement
DELETE FROM bec_dwh.DIM_ITEM_STD_COSTS
WHERE EXISTS 
(
    SELECT 1
    FROM bec_ods.cst_standard_costs a
    WHERE NVL(DIM_ITEM_STD_COSTS.COST_UPDATE_ID, 0) = NVL(a.COST_UPDATE_ID, 0)
    AND NVL(DIM_ITEM_STD_COSTS.INVENTORY_ITEM_ID, 0) = NVL(a.INVENTORY_ITEM_ID, 0)
    AND NVL(DIM_ITEM_STD_COSTS.ORGANIZATION_ID, 0) = NVL(a.ORGANIZATION_ID, 0)
    AND a.kca_seq_date > (
        SELECT (executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_item_std_costs' AND batch_name = 'po')
);
commit;
--insert
insert into bec_dwh.DIM_ITEM_STD_COSTS
(
select
	a.cost_update_id,
	a.inventory_item_id,
	a.organization_id,
	a.organization_id || '-' || a.inventory_item_id item_category_set1,
	a.organization_id || '-' || a.inventory_item_id item_category_set2,
	TRUNC (a.last_update_date) last_update_date,
	a.standard_cost,
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-' || nvl(a.COST_UPDATE_ID, 0)
	|| '-' || nvl(a.INVENTORY_ITEM_ID, 0)
	|| '-' || nvl(a.ORGANIZATION_ID, 0)	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
bec_ods.cst_standard_costs a
where 
a.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info
where dw_table_name ='dim_item_std_costs' and batch_name = 'po')
);
commit;
--soft delete statement
update bec_dwh.DIM_ITEM_STD_COSTS set is_deleted_flg = 'Y'
WHERE NOT EXISTS 
(
    SELECT 1
    FROM bec_ods.cst_standard_costs a
    WHERE NVL(DIM_ITEM_STD_COSTS.COST_UPDATE_ID, 0) = NVL(a.COST_UPDATE_ID, 0)
    AND NVL(DIM_ITEM_STD_COSTS.INVENTORY_ITEM_ID, 0) = NVL(a.INVENTORY_ITEM_ID, 0)
    AND NVL(DIM_ITEM_STD_COSTS.ORGANIZATION_ID, 0) = NVL(a.ORGANIZATION_ID, 0)
    AND a.is_deleted_flg <> 'Y'
);
commit;
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_item_std_costs'
	and batch_name = 'po';

commit;