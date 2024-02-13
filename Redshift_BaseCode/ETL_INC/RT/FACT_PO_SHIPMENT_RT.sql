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

Truncate table bec_dwh_rpt.FACT_PO_SHIPMENT_RT;

Insert Into bec_dwh_rpt.fact_po_shipment_rt
	(SELECT fact_po_shipment.*,
  "dim_inv_category"."item_category_segment1" AS "inv_category_segment1 ",
  "dim_inv_category"."item_category_segment2" AS "inv_category_segment2 "
  FROM "bec_dwh"."fact_po_shipment" "fact_po_shipment" 
  LEFT JOIN "bec_dwh"."dim_inv_item_category_set" "dim_inv_category" ON (("fact_po_shipment"."item_id" = "dim_inv_category"."inventory_item_id") 
  AND ("fact_po_shipment"."to_organization_id" = "dim_inv_category"."organization_id") AND (1 = "dim_inv_category"."category_set_id"))
  );
 end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_shipment_rt'
	and batch_name = 'po';

commit;