/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh_rpt.FACT_INV_ONHAND_RT;

Insert Into bec_dwh_rpt.FACT_INV_ONHAND_RT 
(
SELECT fio.*,
       diic.category_set_name,
       diic.category_set_desc,
       diic.item_category_segment1,
       diic.item_category_segment2,
       diic.item_category_segment3,
       diic.item_category_segment4,
       diic.item_category_desc,
       diss.plan_id ,
       diss.safety_stock_quantity 
  FROM bec_dwh.FACT_INV_ONHAND fio,
       bec_dwh.dim_inv_item_category_set diic,
       bec_dwh.DIM_MSC_SAFTY_STOCK diss 
  WHERE fio.inventory_item_id = diic.inventory_item_id(+)
  and fio.organization_id = diic.organization_id(+)
  and diic.category_set_id(+) = 1
  and fio.inventory_item_id = diss.inventory_item_id(+)
  and fio.organization_id = diss.organization_id(+)
  and diss.plan_id(+) = 40029
 ) ;
 
 end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_onhand_rt'
	and batch_name = 'inv';

commit;
