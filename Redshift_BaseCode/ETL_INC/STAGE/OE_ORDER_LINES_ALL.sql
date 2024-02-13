/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;

TRUNCATE TABLE bec_ods_stg.OE_ORDER_LINES_ALL;

insert into	bec_ods_stg.OE_ORDER_LINES_ALL
    (line_id,
	org_id,
	header_id,
	line_type_id,
	line_number,
	ordered_item,
	request_date,
	promise_date,
	schedule_ship_date,
	order_quantity_uom,
	pricing_quantity,
	pricing_quantity_uom,
	cancelled_quantity,
	shipped_quantity,
	ordered_quantity,
	fulfilled_quantity,
	shipping_quantity,
	shipping_quantity_uom,
	delivery_lead_time,
	tax_exempt_flag,
	tax_exempt_number,
	tax_exempt_reason_code,
	ship_from_org_id,
	ship_to_org_id,
	invoice_to_org_id,
	deliver_to_org_id,
	ship_to_contact_id,
	deliver_to_contact_id,
	invoice_to_contact_id,
	intmed_ship_to_org_id,
	intmed_ship_to_contact_id,
	sold_from_org_id,
	sold_to_org_id,
	cust_po_number,
	ship_tolerance_above,
	ship_tolerance_below,
	demand_bucket_type_code,
	veh_cus_item_cum_key_id,
	rla_schedule_type_code,
	customer_dock_code,
	customer_job,
	customer_production_line,
	cust_model_serial_number,
	project_id,
	task_id,
	inventory_item_id,
	tax_date,
	tax_code,
	tax_rate,
	invoice_interface_status_code,
	demand_class_code,
	price_list_id,
	pricing_date,
	shipment_number,
	agreement_id,
	shipment_priority_code,
	shipping_method_code,
	freight_carrier_code,
	freight_terms_code,
	fob_point_code,
	tax_point_code,
	payment_term_id,
	invoicing_rule_id,
	accounting_rule_id,
	source_document_type_id,
	orig_sys_document_ref,
	source_document_id,
	orig_sys_line_ref,
	source_document_line_id,
	reference_line_id,
	reference_type,
	reference_header_id,
	item_revision,
	unit_selling_price,
	unit_list_price,
	tax_value,
	context,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	pricing_context,
	pricing_attribute1,
	pricing_attribute2,
	pricing_attribute3,
	pricing_attribute4,
	pricing_attribute5,
	pricing_attribute6,
	pricing_attribute7,
	pricing_attribute8,
	pricing_attribute9,
	pricing_attribute10,
	industry_context,
	industry_attribute1,
	industry_attribute2,
	industry_attribute3,
	industry_attribute4,
	industry_attribute5,
	industry_attribute6,
	industry_attribute7,
	industry_attribute8,
	industry_attribute9,
	industry_attribute10,
	industry_attribute11,
	industry_attribute13,
	industry_attribute12,
	industry_attribute14,
	industry_attribute15,
	industry_attribute16,
	industry_attribute17,
	industry_attribute18,
	industry_attribute19,
	industry_attribute20,
	industry_attribute21,
	industry_attribute22,
	industry_attribute23,
	industry_attribute24,
	industry_attribute25,
	industry_attribute26,
	industry_attribute27,
	industry_attribute28,
	industry_attribute29,
	industry_attribute30,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	top_model_line_id,
	link_to_line_id,
	component_sequence_id,
	component_code,
	config_display_sequence,
	sort_order,
	item_type_code,
	option_number,
	option_flag,
	dep_plan_required_flag,
	visible_demand_flag,
	line_category_code,
	actual_shipment_date,
	customer_trx_line_id,
	return_context,
	return_attribute1,
	return_attribute2,
	return_attribute3,
	return_attribute4,
	return_attribute5,
	return_attribute6,
	return_attribute7,
	return_attribute8,
	return_attribute9,
	return_attribute10,
	return_attribute11,
	return_attribute12,
	return_attribute13,
	return_attribute14,
	return_attribute15,
	actual_arrival_date,
	ato_line_id,
	auto_selected_quantity,
	component_number,
	earliest_acceptable_date,
	explosion_date,
	latest_acceptable_date,
	model_group_number,
	schedule_arrival_date,
	ship_model_complete_flag,
	schedule_status_code,
	source_type_code,
	cancelled_flag,
	open_flag,
	booked_flag,
	salesrep_id,
	return_reason_code,
	arrival_set_id,
	ship_set_id,
	split_from_line_id,
	cust_production_seq_num,
	authorized_to_ship_flag,
	over_ship_reason_code,
	over_ship_resolved_flag,
	ordered_item_id,
	item_identifier_type,
	configuration_id,
	commitment_id,
	shipping_interfaced_flag,
	credit_invoice_line_id,
	first_ack_code,
	first_ack_date,
	last_ack_code,
	last_ack_date,
	planning_priority,
	order_source_id,
	orig_sys_shipment_ref,
	change_sequence,
	drop_ship_flag,
	customer_line_number,
	customer_shipment_number,
	customer_item_net_price,
	customer_payment_term_id,
	fulfilled_flag,
	end_item_unit_number,
	config_header_id,
	config_rev_nbr,
	mfg_component_sequence_id,
	shipping_instructions,
	packing_instructions,
	invoiced_quantity,
	reference_customer_trx_line_id,
	split_by,
	line_set_id,
	service_txn_reason_code,
	service_txn_comments,
	service_duration,
	service_start_date,
	service_end_date,
	service_coterminate_flag,
	unit_list_percent,
	unit_selling_percent,
	unit_percent_base_price,
	service_number,
	service_period,
	shippable_flag,
	model_remnant_flag,
	re_source_flag,
	flow_status_code,
	tp_context,
	tp_attribute1,
	tp_attribute2,
	tp_attribute3,
	tp_attribute4,
	tp_attribute5,
	tp_attribute6,
	tp_attribute7,
	tp_attribute8,
	tp_attribute9,
	tp_attribute10,
	tp_attribute11,
	tp_attribute12,
	tp_attribute13,
	tp_attribute14,
	tp_attribute15,
	fulfillment_method_code,
	marketing_source_code_id,
	service_reference_type_code,
	service_reference_line_id,
	service_reference_system_id,
	calculate_price_flag,
	upgraded_flag,
	revenue_amount,
	fulfillment_date,
	preferred_grade,
	ordered_quantity2,
	ordered_quantity_uom2,
	shipping_quantity2,
	cancelled_quantity2,
	shipped_quantity2,
	shipping_quantity_uom2,
	fulfilled_quantity2,
	mfg_lead_time,
	lock_control,
	subinventory,
	unit_list_price_per_pqty,
	unit_selling_price_per_pqty,
	price_request_code,
	original_inventory_item_id,
	original_ordered_item_id,
	original_ordered_item,
	original_item_identifier_type,
	item_substitution_type_code,
	override_atp_date_code,
	late_demand_penalty_factor,
	accounting_rule_duration,
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
	user_item_description,
	unit_cost,
	item_relationship_type,
	blanket_line_number,
	blanket_number,
	blanket_version_number,
	sales_document_type_code,
	firm_demand_flag,
	earliest_ship_date,
	transaction_phase_code,
	source_document_version_number,
	payment_type_code,
	minisite_id,
	end_customer_id,
	end_customer_contact_id,
	end_customer_site_use_id,
	ib_owner,
	ib_current_location,
	ib_installed_at_location,
	retrobill_request_id,
	original_list_price,
	service_credit_eligible_code,
	order_firmed_date,
	actual_fulfillment_date,
	charge_periodicity_code,
	contingency_id,
	revrec_event_code,
	revrec_expiration_days,
	accepted_quantity,
	accepted_by,
	revrec_comments,
	revrec_reference_document,
	revrec_signature,
	revrec_signature_date,
	revrec_implicit_flag,
	billing_plan_header_id,
	bypass_sch_flag,
	container_number,
	equipment_id,
	fulfillment_base,
	inst_id,
	pre_exploded_flag,
	require_billing_validation,
	service_bill_option_code,
	service_bill_profile_id,
	service_cov_template_id,
	service_first_period_amount,
	service_first_period_enddate,
	service_subs_template_id,
	source_order_line_id,
	subscription_enable_flag,
	tax_line_value,
	vrm_last_update_date,
	kca_operation,
	KCA_SEQ_ID
	,kca_seq_date)
(select
	line_id,
	org_id,
	header_id,
	line_type_id,
	line_number,
	ordered_item,
	request_date,
	promise_date,
	schedule_ship_date,
	order_quantity_uom,
	pricing_quantity,
	pricing_quantity_uom,
	cancelled_quantity,
	shipped_quantity,
	ordered_quantity,
	fulfilled_quantity,
	shipping_quantity,
	shipping_quantity_uom,
	delivery_lead_time,
	tax_exempt_flag,
	tax_exempt_number,
	tax_exempt_reason_code,
	ship_from_org_id,
	ship_to_org_id,
	invoice_to_org_id,
	deliver_to_org_id,
	ship_to_contact_id,
	deliver_to_contact_id,
	invoice_to_contact_id,
	intmed_ship_to_org_id,
	intmed_ship_to_contact_id,
	sold_from_org_id,
	sold_to_org_id,
	cust_po_number,
	ship_tolerance_above,
	ship_tolerance_below,
	demand_bucket_type_code,
	veh_cus_item_cum_key_id,
	rla_schedule_type_code,
	customer_dock_code,
	customer_job,
	customer_production_line,
	cust_model_serial_number,
	project_id,
	task_id,
	inventory_item_id,
	tax_date,
	tax_code,
	tax_rate,
	invoice_interface_status_code,
	demand_class_code,
	price_list_id,
	pricing_date,
	shipment_number,
	agreement_id,
	shipment_priority_code,
	shipping_method_code,
	freight_carrier_code,
	freight_terms_code,
	fob_point_code,
	tax_point_code,
	payment_term_id,
	invoicing_rule_id,
	accounting_rule_id,
	source_document_type_id,
	orig_sys_document_ref,
	source_document_id,
	orig_sys_line_ref,
	source_document_line_id,
	reference_line_id,
	reference_type,
	reference_header_id,
	item_revision,
	unit_selling_price,
	unit_list_price,
	tax_value,
	context,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	pricing_context,
	pricing_attribute1,
	pricing_attribute2,
	pricing_attribute3,
	pricing_attribute4,
	pricing_attribute5,
	pricing_attribute6,
	pricing_attribute7,
	pricing_attribute8,
	pricing_attribute9,
	pricing_attribute10,
	industry_context,
	industry_attribute1,
	industry_attribute2,
	industry_attribute3,
	industry_attribute4,
	industry_attribute5,
	industry_attribute6,
	industry_attribute7,
	industry_attribute8,
	industry_attribute9,
	industry_attribute10,
	industry_attribute11,
	industry_attribute13,
	industry_attribute12,
	industry_attribute14,
	industry_attribute15,
	industry_attribute16,
	industry_attribute17,
	industry_attribute18,
	industry_attribute19,
	industry_attribute20,
	industry_attribute21,
	industry_attribute22,
	industry_attribute23,
	industry_attribute24,
	industry_attribute25,
	industry_attribute26,
	industry_attribute27,
	industry_attribute28,
	industry_attribute29,
	industry_attribute30,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	top_model_line_id,
	link_to_line_id,
	component_sequence_id,
	component_code,
	config_display_sequence,
	sort_order,
	item_type_code,
	option_number,
	option_flag,
	dep_plan_required_flag,
	visible_demand_flag,
	line_category_code,
	actual_shipment_date,
	customer_trx_line_id,
	return_context,
	return_attribute1,
	return_attribute2,
	return_attribute3,
	return_attribute4,
	return_attribute5,
	return_attribute6,
	return_attribute7,
	return_attribute8,
	return_attribute9,
	return_attribute10,
	return_attribute11,
	return_attribute12,
	return_attribute13,
	return_attribute14,
	return_attribute15,
	actual_arrival_date,
	ato_line_id,
	auto_selected_quantity,
	component_number,
	earliest_acceptable_date,
	explosion_date,
	latest_acceptable_date,
	model_group_number,
	schedule_arrival_date,
	ship_model_complete_flag,
	schedule_status_code,
	source_type_code,
	cancelled_flag,
	open_flag,
	booked_flag,
	salesrep_id,
	return_reason_code,
	arrival_set_id,
	ship_set_id,
	split_from_line_id,
	cust_production_seq_num,
	authorized_to_ship_flag,
	over_ship_reason_code,
	over_ship_resolved_flag,
	ordered_item_id,
	item_identifier_type,
	configuration_id,
	commitment_id,
	shipping_interfaced_flag,
	credit_invoice_line_id,
	first_ack_code,
	first_ack_date,
	last_ack_code,
	last_ack_date,
	planning_priority,
	order_source_id,
	orig_sys_shipment_ref,
	change_sequence,
	drop_ship_flag,
	customer_line_number,
	customer_shipment_number,
	customer_item_net_price,
	customer_payment_term_id,
	fulfilled_flag,
	end_item_unit_number,
	config_header_id,
	config_rev_nbr,
	mfg_component_sequence_id,
	shipping_instructions,
	packing_instructions,
	invoiced_quantity,
	reference_customer_trx_line_id,
	split_by,
	line_set_id,
	service_txn_reason_code,
	service_txn_comments,
	service_duration,
	service_start_date,
	service_end_date,
	service_coterminate_flag,
	unit_list_percent,
	unit_selling_percent,
	unit_percent_base_price,
	service_number,
	service_period,
	shippable_flag,
	model_remnant_flag,
	re_source_flag,
	flow_status_code,
	tp_context,
	tp_attribute1,
	tp_attribute2,
	tp_attribute3,
	tp_attribute4,
	tp_attribute5,
	tp_attribute6,
	tp_attribute7,
	tp_attribute8,
	tp_attribute9,
	tp_attribute10,
	tp_attribute11,
	tp_attribute12,
	tp_attribute13,
	tp_attribute14,
	tp_attribute15,
	fulfillment_method_code,
	marketing_source_code_id,
	service_reference_type_code,
	service_reference_line_id,
	service_reference_system_id,
	calculate_price_flag,
	upgraded_flag,
	revenue_amount,
	fulfillment_date,
	preferred_grade,
	ordered_quantity2,
	ordered_quantity_uom2,
	shipping_quantity2,
	cancelled_quantity2,
	shipped_quantity2,
	shipping_quantity_uom2,
	fulfilled_quantity2,
	mfg_lead_time,
	lock_control,
	subinventory,
	unit_list_price_per_pqty,
	unit_selling_price_per_pqty,
	price_request_code,
	original_inventory_item_id,
	original_ordered_item_id,
	original_ordered_item,
	original_item_identifier_type,
	item_substitution_type_code,
	override_atp_date_code,
	late_demand_penalty_factor,
	accounting_rule_duration,
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
	user_item_description,
	unit_cost,
	item_relationship_type,
	blanket_line_number,
	blanket_number,
	blanket_version_number,
	sales_document_type_code,
	firm_demand_flag,
	earliest_ship_date,
	transaction_phase_code,
	source_document_version_number,
	payment_type_code,
	minisite_id,
	end_customer_id,
	end_customer_contact_id,
	end_customer_site_use_id,
	ib_owner,
	ib_current_location,
	ib_installed_at_location,
	retrobill_request_id,
	original_list_price,
	service_credit_eligible_code,
	order_firmed_date,
	actual_fulfillment_date,
	charge_periodicity_code,
	contingency_id,
	revrec_event_code,
	revrec_expiration_days,
	accepted_quantity,
	accepted_by,
	revrec_comments,
	revrec_reference_document,
	revrec_signature,
	revrec_signature_date,
	revrec_implicit_flag,
	billing_plan_header_id,
	bypass_sch_flag,
	container_number,
	equipment_id,
	fulfillment_base,
	inst_id,
	pre_exploded_flag,
	require_billing_validation,
	service_bill_option_code,
	service_bill_profile_id,
	service_cov_template_id,
	service_first_period_amount,
	service_first_period_enddate,
	service_subs_template_id,
	source_order_line_id,
	subscription_enable_flag,
	tax_line_value,
	vrm_last_update_date,
	kca_operation,
	KCA_SEQ_ID
	,kca_seq_date
from
	bec_raw_dl_ext.OE_ORDER_LINES_ALL 
where kca_operation != 'DELETE'     and nvl(kca_seq_id,'') != ''
	and (NVL(LINE_ID,0),KCA_SEQ_ID) in 
	(select NVL(LINE_ID,0) AS LINE_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.OE_ORDER_LINES_ALL 
     where kca_operation != 'DELETE'    and nvl(kca_seq_id,'') != ''
     group by NVL(LINE_ID,0))
     and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='oe_order_lines_all')
            )
	 );
END;