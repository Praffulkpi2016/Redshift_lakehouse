/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach to ODS.
# File Version: KPI v1.0
*/


begin;
	
	DROP TABLE if exists bec_ods.MSC_ORDERS_V;
	
	CREATE TABLE IF NOT EXISTS bec_ods.MSC_ORDERS_V
	(
		source_table VARCHAR(12)   ENCODE lzo
		,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,created_by NUMERIC(15,0)   ENCODE az64
		,inventory_item_id NUMERIC(15,0)   ENCODE az64
		,organization_id NUMERIC(15,0)   ENCODE az64
		,organization_code VARCHAR(7)   ENCODE lzo
		,sr_instance_id NUMERIC(15,0)   ENCODE az64
		,plan_id NUMERIC(15,0)   ENCODE az64
		,compile_designator VARCHAR(10)   ENCODE lzo
		,"action" VARCHAR(4000)   ENCODE lzo
		,new_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,old_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,new_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,order_number VARCHAR(4000)   ENCODE lzo
		,order_type NUMERIC(15,0)   ENCODE az64
		,order_type_text VARCHAR(4000)   ENCODE lzo
		,quantity_rate NUMERIC(28,10)   ENCODE az64
		,new_order_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,rescheduled_flag NUMERIC(15,0)   ENCODE az64
		,new_processing_days NUMERIC(15,0)   ENCODE az64
		,new_dock_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,item_segments VARCHAR(250)   ENCODE lzo
		,planner_code VARCHAR(10)   ENCODE lzo
		,build_in_wip_flag NUMERIC(15,0)   ENCODE az64
		,purchasing_enabled_flag NUMERIC(15,0)   ENCODE az64
		,planning_make_buy_code NUMERIC(15,0)   ENCODE az64
		,days_from_today NUMERIC(15,0)   ENCODE az64
		,wip_supply_type NUMERIC(15,0)   ENCODE az64 
		,source_organization_id NUMERIC(15,0)   ENCODE az64
		,full_pegging NUMERIC(28,10)   ENCODE az64
		,source_vendor_name VARCHAR(4000)   ENCODE lzo
		,source_vendor_site_code VARCHAR(4000)   ENCODE lzo
		,supplier_name VARCHAR(4000)   ENCODE lzo
		,schedule_compression_days NUMERIC(15,0)   ENCODE az64
		,release_time_fence_code NUMERIC(28,10)   ENCODE az64
		,buyer_name VARCHAR(240)   ENCODE lzo
		,description VARCHAR(240)   ENCODE lzo
		,category_set_id NUMERIC(15,0)   ENCODE az64
		,category_name VARCHAR(250)   ENCODE lzo
		,promise_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,request_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,customer_id NUMERIC(15,0)   ENCODE az64
		,customer_name VARCHAR(4000)   ENCODE lzo
		,lot_number VARCHAR(80)   ENCODE lzo
		,subinventory_code VARCHAR(10)   ENCODE lzo
		,need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,list_price NUMERIC(28,10)   ENCODE az64
		,standard_cost NUMERIC(28,10)   ENCODE az64
		,ship_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,quantity NUMERIC(28,10)   ENCODE az64
		,po_line_id NUMERIC(15,0)   ENCODE az64
		,reschedule_days NUMERIC(15,0)   ENCODE az64
		,old_need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,orders_days_late NUMERIC(15,0)   ENCODE az64
		,shipment_id NUMERIC(15,0)   ENCODE az64
		,receiving_calendar VARCHAR(15)   ENCODE lzo
		,intransit_lead_time NUMERIC(28,10)   ENCODE az64
		,vmi_flag NUMERIC(15,0)   ENCODE az64
		,fill_kill_flag NUMERIC(15,0)   ENCODE az64
		,within_rel_time_fence NUMERIC(28,10)   ENCODE az64
		,amount NUMERIC(28,10)   ENCODE az64
		,so_line_split NUMERIC(28,10)   ENCODE az64
		,delivery_price NUMERIC(28,10)   ENCODE az64
		,comments VARCHAR(4000)   ENCODE lzo
		,transaction_id NUMERIC(15,0)   ENCODE az64
		,using_assembly_segments varchar(4000) ENCODE lzo
		,category_id NUMERIC(15,0)   ENCODE az64
		,using_assembly_item_id NUMERIC(15,0)   ENCODE az64
		,kca_operation VARCHAR(10)   ENCODE lzo
		,is_deleted_flg VARCHAR(2) ENCODE lzo
		,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	)
	DISTSTYLE
	auto;
	
	INSERT INTO bec_ods.MSC_ORDERS_V (
		source_table,
		creation_date,
		created_by,
		inventory_item_id,
		organization_id,
		organization_code,
		sr_instance_id,
		plan_id,
		compile_designator,
		"action",
		new_due_date,
		old_due_date,
		new_start_date,
		order_number,
		order_type,
		order_type_text,
		quantity_rate,
		new_order_date,
		rescheduled_flag,
		new_processing_days,
		new_dock_date,
		item_segments,
		planner_code,
		build_in_wip_flag,
		purchasing_enabled_flag,
		planning_make_buy_code,
		days_from_today,
		wip_supply_type,
		source_organization_id,
		full_pegging,
		source_vendor_name,
		source_vendor_site_code,
		supplier_name,
		schedule_compression_days,
		release_time_fence_code,
		buyer_name,
		description,
		category_set_id,
		category_name,
		promise_date,
		request_date,
		customer_id,
		customer_name,
		lot_number,
		subinventory_code,
		need_by_date,
		list_price,
		standard_cost,
		ship_date,
		quantity,
		po_line_id,
		reschedule_days,
		old_need_by_date,
		orders_days_late,
		shipment_id,
		receiving_calendar,
		intransit_lead_time,
		vmi_flag,
		fill_kill_flag,
		within_rel_time_fence,
		amount,
		so_line_split,
		delivery_price,
		comments
		,transaction_id
		,using_assembly_segments,
			category_id,
	using_assembly_item_id
		,kca_operation
		,is_deleted_flg
		,kca_seq_id
		,kca_seq_date  
	)
    SELECT
	source_table,
	creation_date,
	created_by,
	inventory_item_id,
	organization_id,
	organization_code,
	sr_instance_id,
	plan_id,
	compile_designator,
	"action",
	new_due_date,
	old_due_date,
	new_start_date,
	order_number,
	order_type,
	order_type_text,
	quantity_rate,
	new_order_date,
	rescheduled_flag,
	new_processing_days,
	new_dock_date,
	item_segments,
	planner_code,
	build_in_wip_flag,
	purchasing_enabled_flag,
	planning_make_buy_code,
	days_from_today,
	wip_supply_type,
	source_organization_id,
	full_pegging,
	source_vendor_name,
	source_vendor_site_code,
	supplier_name,
	schedule_compression_days,
	release_time_fence_code,
	buyer_name,
	description,
	category_set_id,
	category_name,
	promise_date,
	request_date,
	customer_id,
	customer_name,
	lot_number,
	subinventory_code,
	need_by_date,
	list_price,
	standard_cost,
	ship_date,
	quantity,
	po_line_id,
	reschedule_days,
	old_need_by_date,
	orders_days_late,
	shipment_id,
	receiving_calendar,
	intransit_lead_time,
	vmi_flag,
	fill_kill_flag,
	within_rel_time_fence,
	amount,
	so_line_split,
	delivery_price,
	comments,
	transaction_id,
	using_assembly_segments,
	category_id,
	using_assembly_item_id,
	kca_operation,
	'N' as IS_DELETED_FLG ,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date  
    FROM
	bec_ods_stg.MSC_ORDERS_V;
	
	end;
	
	
	UPDATE bec_etl_ctrl.batch_ods_info
	SET
	load_type = 'I',
	last_refresh_date = getdate()
	WHERE
	ods_table_name = 'msc_orders_v';
	
	commit;	