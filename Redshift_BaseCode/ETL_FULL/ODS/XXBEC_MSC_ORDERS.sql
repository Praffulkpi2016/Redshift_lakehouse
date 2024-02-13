/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.XXBEC_MSC_ORDERS;

CREATE TABLE IF NOT EXISTS bec_ods.XXBEC_MSC_ORDERS
(

   	plan_id NUMERIC(15,0)   ENCODE az64
	,compile_designator VARCHAR(10)   ENCODE lzo
	,organization_code VARCHAR(7)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,item_segments VARCHAR(250)   ENCODE lzo
	,requested_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,requested_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_type_text VARCHAR(4000)   ENCODE lzo
	,new_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ship_set_name VARCHAR(30)   ENCODE lzo
	,arrival_set_name VARCHAR(30)   ENCODE lzo
	,schedule_arrival_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,schedule_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,planned_arrival_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,latest_acceptable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,quantity_rate NUMERIC(28,10)   ENCODE az64
	,qty_by_due_date NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,shipment_id NUMERIC(15,0)   ENCODE az64
	,reschedule_days NUMERIC(15,0)   ENCODE az64
	,old_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,disposition_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,original_item_name VARCHAR(4000)   ENCODE lzo
	,prev_subst_item VARCHAR(4000)   ENCODE lzo
	,prev_subst_org VARCHAR(4000)   ENCODE lzo
	,original_item_qty NUMERIC(28,10)   ENCODE az64
	,last_unit_completion_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,line_code VARCHAR(4000)   ENCODE lzo
	,expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_number VARCHAR(4000)   ENCODE lzo
	,"action" VARCHAR(4000)   ENCODE lzo
	,mrp_planning_code_text VARCHAR(4000)   ENCODE lzo
	,old_order_quantity NUMERIC(28,10)   ENCODE az64
	,orig_org_code VARCHAR(4000)   ENCODE lzo
	,dest_org_code VARCHAR(4000)   ENCODE lzo
	,dest_org_id NUMERIC(15,0)   ENCODE az64
	,dest_inst_id NUMERIC(15,0)   ENCODE az64
	,ship_method VARCHAR(30)   ENCODE lzo
	,orig_ship_method VARCHAR(30)   ENCODE lzo
	,source_vendor_name VARCHAR(4000)   ENCODE lzo
	,source_vendor_site_code VARCHAR(4000)   ENCODE lzo
	,planning_group VARCHAR(30)   ENCODE lzo
	,schedule_group_name VARCHAR(30)   ENCODE lzo
	,quantity NUMERIC(28,10)   ENCODE az64
	,ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,assembly_demand_comp_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,planner_code VARCHAR(10)   ENCODE lzo
	,order_type NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
	,source_vendor_site_id NUMERIC(15,0)   ENCODE az64
	,source_vendor_id NUMERIC(15,0)   ENCODE az64
	,supplier_name VARCHAR(4000)   ENCODE lzo
	,supplier_site_code VARCHAR(4000)   ENCODE lzo
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,mrp_planning_code NUMERIC(15,0)   ENCODE az64
	,source_organization_id NUMERIC(15,0)   ENCODE az64
	,status_code NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,customer_name VARCHAR(4000)   ENCODE lzo
	,ship_to_site_id NUMERIC(15,0 )   ENCODE az64
	,ship_to_site_name VARCHAR(4000)   ENCODE lzo
	,customer_site_id NUMERIC(15,0)   ENCODE az64
	,customer_site_name VARCHAR(4000)   ENCODE lzo
	,promise_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,promise_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,category_set_id NUMERIC(15,0)   ENCODE az64
	,sales_order_line_id NUMERIC(15,0)   ENCODE az64
	,ship_set_id NUMERIC(15,0)   ENCODE az64
	,customer_po_number VARCHAR(4000)   ENCODE lzo
	,customer_po_line_number VARCHAR(4000)   ENCODE lzo
	,buyer_name VARCHAR(240)   ENCODE lzo
	,source_table VARCHAR(12)   ENCODE lzo
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,new_order_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_dock_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.XXBEC_MSC_ORDERS (
	plan_id,
	compile_designator,
	organization_code,
	organization_id,
	item_segments,
	requested_start_date,
	requested_completion_date,
	order_type_text,
	new_due_date,
	ship_set_name,
	arrival_set_name,
	schedule_arrival_date,
	schedule_ship_date,
	actual_start_date,
	planned_arrival_date,
	latest_acceptable_date,
	quantity_rate,
	qty_by_due_date,
	po_line_id,
	shipment_id,
	reschedule_days,
	old_due_date,
	sr_instance_id,
	disposition_id,
	description,
	original_item_name,
	prev_subst_item,
	prev_subst_org,
	original_item_qty,
	last_unit_completion_date,
	line_code,
	expiration_date,
	order_number,
	"action",
	mrp_planning_code_text,
	old_order_quantity,
	orig_org_code,
	dest_org_code,
	dest_org_id,
	dest_inst_id,
	ship_method,
	orig_ship_method,
	source_vendor_name,
	source_vendor_site_code,
	planning_group,
	schedule_group_name,
	quantity,
	ship_date,
	assembly_demand_comp_date,
	planner_code,
	order_type,
	vendor_id,
	vendor_site_id,
	category_id,
	source_vendor_site_id,
	source_vendor_id,
	supplier_name,
	supplier_site_code,
	subinventory_code,
	mrp_planning_code,
	source_organization_id,
	status_code,
	customer_id,
	customer_name,
	ship_to_site_id,
	ship_to_site_name,
	customer_site_id,
	customer_site_name,
	promise_date,
	promise_ship_date,
	original_need_by_date,
	request_date,
	request_ship_date,
	category_set_id,
	sales_order_line_id,
	ship_set_id,
	customer_po_number,
	customer_po_line_number,
	buyer_name,
	source_table,
	transaction_id,
	inventory_item_id,
	new_order_date,
	new_dock_date,
	new_start_date,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
	plan_id,
	compile_designator,
	organization_code,
	organization_id,
	item_segments,
	requested_start_date,
	requested_completion_date,
	order_type_text,
	new_due_date,
	ship_set_name,
	arrival_set_name,
	schedule_arrival_date,
	schedule_ship_date,
	actual_start_date,
	planned_arrival_date,
	latest_acceptable_date,
	quantity_rate,
	qty_by_due_date,
	po_line_id,
	shipment_id,
	reschedule_days,
	old_due_date,
	sr_instance_id,
	disposition_id,
	description,
	original_item_name,
	prev_subst_item,
	prev_subst_org,
	original_item_qty,
	last_unit_completion_date,
	line_code,
	expiration_date,
	order_number,
	"action",
	mrp_planning_code_text,
	old_order_quantity,
	orig_org_code,
	dest_org_code,
	dest_org_id,
	dest_inst_id,
	ship_method,
	orig_ship_method,
	source_vendor_name,
	source_vendor_site_code,
	planning_group,
	schedule_group_name,
	quantity,
	ship_date,
	assembly_demand_comp_date,
	planner_code,
	order_type,
	vendor_id,
	vendor_site_id,
	category_id,
	source_vendor_site_id,
	source_vendor_id,
	supplier_name,
	supplier_site_code,
	subinventory_code,
	mrp_planning_code,
	source_organization_id,
	status_code,
	customer_id,
	customer_name,
	ship_to_site_id,
	ship_to_site_name,
	customer_site_id,
	customer_site_name,
	promise_date,
	promise_ship_date,
	original_need_by_date,
	request_date,
	request_ship_date,
	category_set_id,
	sales_order_line_id,
	ship_set_id,
	customer_po_number,
	customer_po_line_number,
	buyer_name,
	source_table,
	transaction_id,
	inventory_item_id,
	new_order_date,
	new_dock_date,
	new_start_date,
	kca_operation,
     'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.XXBEC_MSC_ORDERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xxbec_msc_orders';
	
COMMIT;