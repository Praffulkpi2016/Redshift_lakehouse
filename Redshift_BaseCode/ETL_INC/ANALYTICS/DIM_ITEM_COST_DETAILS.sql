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

drop table if exists bec_dwh.DIM_ITEM_COST_DETAILS;

create table bec_dwh.DIM_ITEM_COST_DETAILS
DISTKEY (organization_id)
SORTKEY (organization_id, inventory_item_id)
as
(
select 
inventory_item_id
,organization_id
,cost_element_id
,cost_type_id
,resource_id
,resource_code
,item_cost
,	'N' as is_deleted_flg,
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
    ||'-'|| nvl(inventory_item_id, 0)
    ||'-'|| nvl(organization_id, 0)
    ||'-'|| nvl(cost_element_id, 0)
	||'-'|| nvl(cost_type_id, 0)
    ||'-'|| nvl(resource_id, 0)	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
SELECT  ccd.inventory_item_id
       ,ccd.organization_id
       ,ccd.cost_element_id
       ,ccd.cost_type_id
       ,ccd.resource_id
       ,br.resource_code
       ,SUM (item_cost) item_cost
FROM  bec_ods.cst_item_cost_details ccd
     ,bec_ods.bom_resources br
WHERE  1 = 1              
AND  ccd.resource_id     = br.resource_id
AND ccd.ORGANIZATION_ID  = br.organization_id 
GROUP BY ccd.inventory_item_id
       ,ccd.organization_id
       ,ccd.cost_element_id
       ,ccd.cost_type_id
       ,ccd.resource_id
       ,br.resource_code
));

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_item_cost_details'
	and batch_name = 'inv';

commit;
