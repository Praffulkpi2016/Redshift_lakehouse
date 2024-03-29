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
begin;

truncate table bec_ods_stg.CS_INCIDENTS_ALL_B;

insert into	bec_ods_stg.CS_INCIDENTS_ALL_B
(
incident_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	creation_time,
	incident_number,
	incident_date,
	open_flag,
	service_entity_id,
	incident_status_id,
	incident_type_id,
	incident_urgency_id,
	incident_severity_id,
	summary,
	responsible_group_id,
	security_level_code,
	incident_owner_id,
	classification_code,
	inventory_item_id,
	component_inventory_item_id,
	customer_id,
	original_customer_id,
	customer_name,
	bill_to_site_use_id,
	purchase_order_num,
	identification_number,
	service_inventory_item_id,
	employee_id,
	filed_by_employee_flag,
	billable_flag,
	ship_to_site_use_id,
	problem_code,
	expected_resolution_date,
	actual_resolution_date,
	problem_description,
	resolution_description,
	running_totals,
	current_contact_person_id,
	initiating_person_id,
	current_contact_name,
	current_contact_area_code,
	current_contact_telephone,
	current_contact_extension,
	current_contact_email_address,
	current_contact_time_diff,
	current_contact_fax_area_code,
	current_contact_fax_number,
	represented_by_employee,
	represented_by_name,
	represented_by_time_difference,
	represented_by_area_code,
	represented_by_telephone,
	represented_by_extension,
	represented_by_fax_area_code,
	represented_by_fax_number,
	represented_by_email_address,
	start_date_active,
	end_date_active,
	internal_contact_id,
	hours_after_gmt,
	initiating_time_difference,
	customer_product_id,
	cp_service_id,
	currently_serviced_flag,
	last_customer_callback_time,
	preventive_maintenance_flag,
	bill_to_title,
	bill_to_customer,
	bill_to_address_line_1,
	bill_to_address_line_2,
	bill_to_address_line_3,
	bill_to_contact,
	ship_to_title,
	ship_to_customer,
	ship_to_address_line_1,
	ship_to_address_line_2,
	ship_to_address_line_3,
	ship_to_contact,
	install_title,
	install_customer,
	install_address_line_1,
	install_address_line_2,
	install_address_line_3,
	install_site_use_id,
	bill_to_contact_id,
	ship_to_contact_id,
	initiating_name,
	initiating_area_code,
	initiating_telephone,
	initiating_extension,
	initiating_fax_area_code,
	initiating_fax_number,
	product_name,
	product_description,
	current_serial_number,
	product_revision,
	customer_number,
	system_id,
	incident_attribute_1,
	incident_attribute_2,
	incident_attribute_3,
	incident_attribute_4,
	incident_attribute_5,
	incident_attribute_6,
	incident_attribute_7,
	incident_attribute_8,
	incident_attribute_9,
	incident_attribute_10,
	incident_attribute_11,
	incident_attribute_12,
	incident_attribute_13,
	incident_attribute_14,
	incident_attribute_15,
	incident_context,
	record_is_valid_flag,
	incident_comments,
	resolution_code,
	rma_number,
	rma_header_id,
	rma_flag,
	org_id,
	original_order_number,
	workflow_process_id,
	web_entry_flag,
	close_date,
	publish_flag,
	make_public_problem,
	make_public_resolution,
	estimate_id,
	estimate_business_group_id,
	interfaced_to_depot_flag,
	qa_collection_id,
	project_id,
	task_id,
	contract_id,
	contract_number,
	contract_service_id,
	resource_type,
	resource_subtype_id,
	account_id,
	kb_type,
	kb_solution_id,
	time_zone_id,
	time_difference,
	customer_po_number,
	owner_group_id,
	customer_ticket_number,
	obligation_date,
	site_id,
	customer_site_id,
	caller_type,
	platform_version_id,
	object_version_number,
	cp_component_id,
	cp_component_version_id,
	cp_subcomponent_id,
	cp_subcomponent_version_id,
	platform_id,
	language_id,
	territory_id,
	cp_revision_id,
	inv_item_revision,
	inv_component_id,
	inv_component_version,
	inv_subcomponent_id,
	inv_subcomponent_version,
	inv_organization_id,
	security_group_id,
	upgraded_status_flag1,
	upgraded_status_flag2,
	upgraded_status_flag3,
	orig_system_reference,
	orig_system_reference_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_number,
	platform_version,
	db_version,
	cust_pref_lang_id,
	tier,
	tier_version,
	category_id,
	operating_system,
	operating_system_version,
	"database",
	group_type,
	group_territory_id,
	owner_assigned_time,
	owner_assigned_flag,
	inv_platform_org_id,
	component_version,
	subcomponent_version,
	comm_pref_code,
	last_update_channel,
	cust_pref_lang_code,
	error_code,
	category_set_id,
	external_reference,
	incident_occurred_date,
	incident_resolved_date,
	inc_responded_by_date,
	incident_location_id,
	incident_address,
	incident_city,
	incident_state,
	incident_country,
	incident_province,
	incident_postal_code,
	incident_county,
	sr_creation_channel,
	def_defect_id,
	def_defect_id2,
	credit_card_number,
	credit_card_type_code,
	credit_card_expiration_date,
	credit_card_holder_fname,
	credit_card_holder_mname,
	credit_card_holder_lname,
	credit_card_id,
	external_attribute_1,
	external_attribute_2,
	external_attribute_3,
	external_attribute_4,
	external_attribute_5,
	external_attribute_6,
	external_attribute_7,
	external_attribute_8,
	external_attribute_9,
	external_attribute_10,
	external_attribute_11,
	external_attribute_12,
	external_attribute_13,
	external_attribute_14,
	external_attribute_15,
	external_context,
	last_update_program_code,
	creation_program_code,
	coverage_type,
	bill_to_account_id,
	ship_to_account_id,
	customer_email_id,
	customer_phone_id,
	bill_to_party_id,
	ship_to_party_id,
	bill_to_site_id,
	ship_to_site_id,
	program_login_id,
	status_flag,
	primary_contact_id,
	incident_point_of_interest,
	incident_cross_street,
	incident_direction_qualifier,
	incident_distance_qualifier,
	incident_distance_qual_uom,
	incident_address2,
	incident_address3,
	incident_address4,
	incident_address_style,
	incident_addr_lines_phonetic,
	incident_po_box_number,
	incident_house_number,
	incident_street_suffix,
	incident_street,
	incident_street_number,
	incident_floor,
	incident_suite,
	incident_postal_plus4_code,
	incident_position,
	incident_location_directions,
	incident_location_description,
	install_site_id,
	owning_department_id,
	item_serial_number,
	incident_location_type,
	incident_last_modified_date,
	unassigned_indicator,
	maint_organization_id,
	instrument_payment_use_id,
	project_task_id,
	sla_date_1,
	sla_date_2,
	sla_date_3,
	sla_date_4,
	sla_date_5,
	sla_date_6,
	sla_duration_1,
	sla_duration_2,
	price_list_header_id,
	expenditure_org_id,
	business_process_id,
	order_header_id,
	rec_problem_code,
	rec_resolution_code,
	template_id 
,	KCA_OPERATION
,	kca_seq_id
,kca_seq_date)

(
	select
incident_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	creation_time,
	incident_number,
	incident_date,
	open_flag,
	service_entity_id,
	incident_status_id,
	incident_type_id,
	incident_urgency_id,
	incident_severity_id,
	summary,
	responsible_group_id,
	security_level_code,
	incident_owner_id,
	classification_code,
	inventory_item_id,
	component_inventory_item_id,
	customer_id,
	original_customer_id,
	customer_name,
	bill_to_site_use_id,
	purchase_order_num,
	identification_number,
	service_inventory_item_id,
	employee_id,
	filed_by_employee_flag,
	billable_flag,
	ship_to_site_use_id,
	problem_code,
	expected_resolution_date,
	actual_resolution_date,
	problem_description,
	resolution_description,
	running_totals,
	current_contact_person_id,
	initiating_person_id,
	current_contact_name,
	current_contact_area_code,
	current_contact_telephone,
	current_contact_extension,
	current_contact_email_address,
	current_contact_time_diff,
	current_contact_fax_area_code,
	current_contact_fax_number,
	represented_by_employee,
	represented_by_name,
	represented_by_time_difference,
	represented_by_area_code,
	represented_by_telephone,
	represented_by_extension,
	represented_by_fax_area_code,
	represented_by_fax_number,
	represented_by_email_address,
	start_date_active,
	end_date_active,
	internal_contact_id,
	hours_after_gmt,
	initiating_time_difference,
	customer_product_id,
	cp_service_id,
	currently_serviced_flag,
	last_customer_callback_time,
	preventive_maintenance_flag,
	bill_to_title,
	bill_to_customer,
	bill_to_address_line_1,
	bill_to_address_line_2,
	bill_to_address_line_3,
	bill_to_contact,
	ship_to_title,
	ship_to_customer,
	ship_to_address_line_1,
	ship_to_address_line_2,
	ship_to_address_line_3,
	ship_to_contact,
	install_title,
	install_customer,
	install_address_line_1,
	install_address_line_2,
	install_address_line_3,
	install_site_use_id,
	bill_to_contact_id,
	ship_to_contact_id,
	initiating_name,
	initiating_area_code,
	initiating_telephone,
	initiating_extension,
	initiating_fax_area_code,
	initiating_fax_number,
	product_name,
	product_description,
	current_serial_number,
	product_revision,
	customer_number,
	system_id,
	incident_attribute_1,
	incident_attribute_2,
	incident_attribute_3,
	incident_attribute_4,
	incident_attribute_5,
	incident_attribute_6,
	incident_attribute_7,
	incident_attribute_8,
	incident_attribute_9,
	incident_attribute_10,
	incident_attribute_11,
	incident_attribute_12,
	incident_attribute_13,
	incident_attribute_14,
	incident_attribute_15,
	incident_context,
	record_is_valid_flag,
	incident_comments,
	resolution_code,
	rma_number,
	rma_header_id,
	rma_flag,
	org_id,
	original_order_number,
	workflow_process_id,
	web_entry_flag,
	close_date,
	publish_flag,
	make_public_problem,
	make_public_resolution,
	estimate_id,
	estimate_business_group_id,
	interfaced_to_depot_flag,
	qa_collection_id,
	project_id,
	task_id,
	contract_id,
	contract_number,
	contract_service_id,
	resource_type,
	resource_subtype_id,
	account_id,
	kb_type,
	kb_solution_id,
	time_zone_id,
	time_difference,
	customer_po_number,
	owner_group_id,
	customer_ticket_number,
	obligation_date,
	site_id,
	customer_site_id,
	caller_type,
	platform_version_id,
	object_version_number,
	cp_component_id,
	cp_component_version_id,
	cp_subcomponent_id,
	cp_subcomponent_version_id,
	platform_id,
	language_id,
	territory_id,
	cp_revision_id,
	inv_item_revision,
	inv_component_id,
	inv_component_version,
	inv_subcomponent_id,
	inv_subcomponent_version,
	inv_organization_id,
	security_group_id,
	upgraded_status_flag1,
	upgraded_status_flag2,
	upgraded_status_flag3,
	orig_system_reference,
	orig_system_reference_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_number,
	platform_version,
	db_version,
	cust_pref_lang_id,
	tier,
	tier_version,
	category_id,
	operating_system,
	operating_system_version,
	"database",
	group_type,
	group_territory_id,
	owner_assigned_time,
	owner_assigned_flag,
	inv_platform_org_id,
	component_version,
	subcomponent_version,
	comm_pref_code,
	last_update_channel,
	cust_pref_lang_code,
	error_code,
	category_set_id,
	external_reference,
	incident_occurred_date,
	incident_resolved_date,
	inc_responded_by_date,
	incident_location_id,
	incident_address,
	incident_city,
	incident_state,
	incident_country,
	incident_province,
	incident_postal_code,
	incident_county,
	sr_creation_channel,
	def_defect_id,
	def_defect_id2,
	credit_card_number,
	credit_card_type_code,
	credit_card_expiration_date,
	credit_card_holder_fname,
	credit_card_holder_mname,
	credit_card_holder_lname,
	credit_card_id,
	external_attribute_1,
	external_attribute_2,
	external_attribute_3,
	external_attribute_4,
	external_attribute_5,
	external_attribute_6,
	external_attribute_7,
	external_attribute_8,
	external_attribute_9,
	external_attribute_10,
	external_attribute_11,
	external_attribute_12,
	external_attribute_13,
	external_attribute_14,
	external_attribute_15,
	external_context,
	last_update_program_code,
	creation_program_code,
	coverage_type,
	bill_to_account_id,
	ship_to_account_id,
	customer_email_id,
	customer_phone_id,
	bill_to_party_id,
	ship_to_party_id,
	bill_to_site_id,
	ship_to_site_id,
	program_login_id,
	status_flag,
	primary_contact_id,
	incident_point_of_interest,
	incident_cross_street,
	incident_direction_qualifier,
	incident_distance_qualifier,
	incident_distance_qual_uom,
	incident_address2,
	incident_address3,
	incident_address4,
	incident_address_style,
	incident_addr_lines_phonetic,
	incident_po_box_number,
	incident_house_number,
	incident_street_suffix,
	incident_street,
	incident_street_number,
	incident_floor,
	incident_suite,
	incident_postal_plus4_code,
	incident_position,
	incident_location_directions,
	incident_location_description,
	install_site_id,
	owning_department_id,
	item_serial_number,
	incident_location_type,
	incident_last_modified_date,
	unassigned_indicator,
	maint_organization_id,
	instrument_payment_use_id,
	project_task_id,
	sla_date_1,
	sla_date_2,
	sla_date_3,
	sla_date_4,
	sla_date_5,
	sla_date_6,
	sla_duration_1,
	sla_duration_2,
	price_list_header_id,
	expenditure_org_id,
	business_process_id,
	order_header_id,
	rec_problem_code,
	rec_resolution_code,
	template_id 
,		KCA_OPERATION
,		kca_seq_id
,kca_seq_date
	from
		bec_raw_dl_ext.CS_INCIDENTS_ALL_B
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (INCIDENT_ID,kca_seq_id) in 
	(select INCIDENT_ID,max(kca_seq_id) from bec_raw_dl_ext.CS_INCIDENTS_ALL_B 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by INCIDENT_ID)
     and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'cs_incidents_all_b')
);
end;