/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.fact_po_consolidated_rt;

create table bec_dwh_rpt.fact_po_consolidated_rt 
	diststyle all as
(SELECT 
 fact_po_consolidated.*,nvl(fact_po_consolidated.amount_billed,0) as amount_billed1,
 nvl(fact_po_consolidated.amount_received,0) as amount_received1,
 dim_purchasing_category.item_category_segment1 as "inv_category_segment1",
 dim_inv_category.item_category_segment1 as "po_category_segment1",
 dim_purchasing_category.item_category_segment2 as "po_category_segment2",
 dim_inv_category.item_category_segment2 as "inv_category_segment2"
 FROM "bec_dwh"."fact_po_consolidated" "fact_po_consolidated"
  LEFT JOIN "bec_dwh"."dim_inv_item_category_set" "dim_inv_category" ON (("fact_po_consolidated"."item_id" = "dim_inv_category"."inventory_item_id") AND ("fact_po_consolidated"."ship_to_organization_id" = "dim_inv_category"."organization_id") AND (1 = "dim_inv_category"."category_set_id"))
  LEFT JOIN "bec_dwh"."dim_inv_item_category_set" "dim_purchasing_category" ON (("fact_po_consolidated"."item_id" = "dim_purchasing_category"."inventory_item_id") AND ("fact_po_consolidated"."ship_to_organization_id" = "dim_purchasing_category"."organization_id") AND (1100000043 = "dim_purchasing_category"."category_set_id"))
  );
  end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_consolidated_rt'
	and batch_name = 'po';

commit;