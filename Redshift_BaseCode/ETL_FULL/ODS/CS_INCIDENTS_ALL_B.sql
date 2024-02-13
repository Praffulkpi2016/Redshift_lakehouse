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

DROP TABLE if exists bec_ods.CS_INCIDENTS_ALL_B;

CREATE TABLE IF NOT EXISTS bec_ods.CS_INCIDENTS_ALL_B
(
	incident_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_time VARCHAR(30)   ENCODE lzo
	,incident_number VARCHAR(64)   ENCODE lzo
	,incident_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,open_flag VARCHAR(1)   ENCODE lzo
	,service_entity_id NUMERIC(15,0)   ENCODE az64
	,incident_status_id NUMERIC(15,0)   ENCODE az64
	,incident_type_id NUMERIC(15,0)   ENCODE az64
	,incident_urgency_id NUMERIC(15,0)   ENCODE az64
	,incident_severity_id NUMERIC(15,0)   ENCODE az64
	,summary VARCHAR(80)   ENCODE lzo
	,responsible_group_id NUMERIC(15,0)   ENCODE az64
	,security_level_code VARCHAR(30)   ENCODE lzo
	,incident_owner_id NUMERIC(15,0)   ENCODE az64
	,classification_code VARCHAR(30)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,component_inventory_item_id NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,original_customer_id NUMERIC(15,0)   ENCODE az64
	,customer_name VARCHAR(50)   ENCODE lzo
	,bill_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,purchase_order_num VARCHAR(50)   ENCODE lzo
	,identification_number VARCHAR(50)   ENCODE lzo
	,service_inventory_item_id NUMERIC(15,0)   ENCODE az64
	,employee_id NUMERIC(15,0)   ENCODE az64
	,filed_by_employee_flag VARCHAR(1)   ENCODE lzo
	,billable_flag VARCHAR(1)   ENCODE lzo
	,ship_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,problem_code VARCHAR(50)   ENCODE lzo
	,expected_resolution_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_resolution_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,problem_description VARCHAR(2000)   ENCODE lzo
	,resolution_description VARCHAR(2000)   ENCODE lzo
	,running_totals NUMERIC(28,10)   ENCODE az64
	,current_contact_person_id NUMERIC(15,0)   ENCODE az64
	,initiating_person_id NUMERIC(15,0)   ENCODE az64
	,current_contact_name VARCHAR(100)   ENCODE lzo
	,current_contact_area_code VARCHAR(10)   ENCODE lzo
	,current_contact_telephone VARCHAR(25)   ENCODE lzo
	,current_contact_extension VARCHAR(20)   ENCODE lzo
	,current_contact_email_address VARCHAR(240)   ENCODE lzo
	,current_contact_time_diff NUMERIC(28,10)   ENCODE az64
	,current_contact_fax_area_code VARCHAR(10)   ENCODE lzo
	,current_contact_fax_number VARCHAR(25)   ENCODE lzo
	,represented_by_employee VARCHAR(1)   ENCODE lzo
	,represented_by_name VARCHAR(100)   ENCODE lzo
	,represented_by_time_difference NUMERIC(28,10)   ENCODE az64
	,represented_by_area_code VARCHAR(10)   ENCODE lzo
	,represented_by_telephone VARCHAR(25)   ENCODE lzo
	,represented_by_extension VARCHAR(20)   ENCODE lzo
	,represented_by_fax_area_code VARCHAR(10)   ENCODE lzo
	,represented_by_fax_number VARCHAR(25)   ENCODE lzo
	,represented_by_email_address VARCHAR(240)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,internal_contact_id NUMERIC(15,0)   ENCODE az64
	,hours_after_gmt NUMERIC(28,10)   ENCODE az64
	,initiating_time_difference NUMERIC(28,10)   ENCODE az64
	,customer_product_id NUMERIC(15,0)   ENCODE az64
	,cp_service_id NUMERIC(15,0)   ENCODE az64
	,currently_serviced_flag VARCHAR(1)   ENCODE lzo
	,last_customer_callback_time VARCHAR(30)   ENCODE lzo
	,preventive_maintenance_flag VARCHAR(1)   ENCODE lzo
	,bill_to_title VARCHAR(50)   ENCODE lzo
	,bill_to_customer VARCHAR(50)   ENCODE lzo
	,bill_to_address_line_1 VARCHAR(240)   ENCODE lzo
	,bill_to_address_line_2 VARCHAR(240)   ENCODE lzo
	,bill_to_address_line_3 VARCHAR(240)   ENCODE lzo
	,bill_to_contact VARCHAR(100)   ENCODE lzo
	,ship_to_title VARCHAR(50)   ENCODE lzo
	,ship_to_customer VARCHAR(50)   ENCODE lzo
	,ship_to_address_line_1 VARCHAR(240)   ENCODE lzo
	,ship_to_address_line_2 VARCHAR(240)   ENCODE lzo
	,ship_to_address_line_3 VARCHAR(240)   ENCODE lzo
	,ship_to_contact VARCHAR(100)   ENCODE lzo
	,install_title VARCHAR(50)   ENCODE lzo
	,install_customer VARCHAR(100)   ENCODE lzo
	,install_address_line_1 VARCHAR(240)   ENCODE lzo
	,install_address_line_2 VARCHAR(240)   ENCODE lzo
	,install_address_line_3 VARCHAR(240)   ENCODE lzo
	,install_site_use_id NUMERIC(15,0)   ENCODE az64
	,bill_to_contact_id NUMERIC(15,0)   ENCODE az64
	,ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,initiating_name VARCHAR(50)   ENCODE lzo
	,initiating_area_code VARCHAR(10)   ENCODE lzo
	,initiating_telephone VARCHAR(25)   ENCODE lzo
	,initiating_extension VARCHAR(20)   ENCODE lzo
	,initiating_fax_area_code VARCHAR(10)   ENCODE lzo
	,initiating_fax_number VARCHAR(25)   ENCODE lzo
	,product_name VARCHAR(240)   ENCODE lzo
	,product_description VARCHAR(240)   ENCODE lzo
	,current_serial_number VARCHAR(50)   ENCODE lzo
	,product_revision VARCHAR(240)   ENCODE lzo
	,customer_number VARCHAR(100)   ENCODE lzo
	,system_id NUMERIC(15,0)   ENCODE az64
	,incident_attribute_1 VARCHAR(150)   ENCODE lzo
	,incident_attribute_2 VARCHAR(150)   ENCODE lzo
	,incident_attribute_3 VARCHAR(150)   ENCODE lzo
	,incident_attribute_4 VARCHAR(150)   ENCODE lzo
	,incident_attribute_5 VARCHAR(150)   ENCODE lzo
	,incident_attribute_6 VARCHAR(150)   ENCODE lzo
	,incident_attribute_7 VARCHAR(150)   ENCODE lzo
	,incident_attribute_8 VARCHAR(150)   ENCODE lzo
	,incident_attribute_9 VARCHAR(150)   ENCODE lzo
	,incident_attribute_10 VARCHAR(150)   ENCODE lzo
	,incident_attribute_11 VARCHAR(150)   ENCODE lzo
	,incident_attribute_12 VARCHAR(150)   ENCODE lzo
	,incident_attribute_13 VARCHAR(150)   ENCODE lzo
	,incident_attribute_14 VARCHAR(150)   ENCODE lzo
	,incident_attribute_15 VARCHAR(150)   ENCODE lzo
	,incident_context VARCHAR(30)   ENCODE lzo
	,record_is_valid_flag VARCHAR(1)   ENCODE lzo
	,incident_comments VARCHAR(255)   ENCODE lzo
	,resolution_code VARCHAR(50)   ENCODE lzo
	,rma_number NUMERIC(15,0)   ENCODE az64
	,rma_header_id NUMERIC(15,0)   ENCODE az64
	,rma_flag VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,original_order_number NUMERIC(28,10)   ENCODE az64
	,workflow_process_id NUMERIC(15,0)   ENCODE az64
	,web_entry_flag VARCHAR(1)   ENCODE lzo
	,close_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,publish_flag VARCHAR(1)   ENCODE lzo
	,make_public_problem VARCHAR(1)   ENCODE lzo
	,make_public_resolution VARCHAR(1)   ENCODE lzo
	,estimate_id NUMERIC(15,0)   ENCODE az64
	,estimate_business_group_id NUMERIC(15,0)   ENCODE az64
	,interfaced_to_depot_flag VARCHAR(1)   ENCODE lzo
	,qa_collection_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,contract_id NUMERIC(15,0)   ENCODE az64
	,contract_number VARCHAR(120)   ENCODE lzo
	,contract_service_id NUMERIC(15,0)   ENCODE az64
	,resource_type VARCHAR(30)   ENCODE lzo
	,resource_subtype_id NUMERIC(15,0)   ENCODE az64
	,account_id NUMERIC(15,0)   ENCODE az64
	,kb_type VARCHAR(15)   ENCODE lzo
	,kb_solution_id VARCHAR(240)   ENCODE lzo
	,time_zone_id NUMERIC(15,0)   ENCODE az64
	,time_difference NUMERIC(28,10)   ENCODE az64
	,customer_po_number VARCHAR(50)   ENCODE lzo
	,owner_group_id NUMERIC(28,10)   ENCODE az64
	,customer_ticket_number VARCHAR(50)   ENCODE lzo
	,obligation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,site_id NUMERIC(15,0)   ENCODE az64
	,customer_site_id NUMERIC(15,0)   ENCODE az64
	,caller_type VARCHAR(30)   ENCODE lzo
	,platform_version_id NUMERIC(15,0)   ENCODE az64
	,object_version_number INTEGER   ENCODE az64
	,cp_component_id NUMERIC(15,0)   ENCODE az64
	,cp_component_version_id NUMERIC(15,0)   ENCODE az64
	,cp_subcomponent_id NUMERIC(15,0)   ENCODE az64
	,cp_subcomponent_version_id NUMERIC(15,0)   ENCODE az64
	,platform_id NUMERIC(15,0)   ENCODE az64
	,language_id NUMERIC(15,0)   ENCODE az64
	,territory_id NUMERIC(15,0)   ENCODE az64
	,cp_revision_id NUMERIC(15,0)   ENCODE az64
	,inv_item_revision VARCHAR(240)   ENCODE lzo
	,inv_component_id NUMERIC(15,0)   ENCODE az64
	,inv_component_version VARCHAR(90)   ENCODE lzo
	,inv_subcomponent_id NUMERIC(15,0)   ENCODE az64
	,inv_subcomponent_version VARCHAR(90)   ENCODE lzo
	,inv_organization_id NUMERIC(15,0)   ENCODE az64
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,upgraded_status_flag1 VARCHAR(1)   ENCODE lzo
	,upgraded_status_flag2 VARCHAR(1)   ENCODE lzo
	,upgraded_status_flag3 VARCHAR(1)   ENCODE lzo
	,orig_system_reference VARCHAR(60)   ENCODE lzo
	,orig_system_reference_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,project_number VARCHAR(120)   ENCODE lzo
	,platform_version VARCHAR(250)   ENCODE lzo
	,db_version VARCHAR(250)   ENCODE lzo
	,cust_pref_lang_id NUMERIC(15,0)   ENCODE az64
	,tier VARCHAR(250)   ENCODE lzo
	,tier_version VARCHAR(250)   ENCODE lzo
	,category_id NUMERIC(15,0)   ENCODE az64
	,operating_system VARCHAR(250)   ENCODE lzo
	,operating_system_version VARCHAR(250)   ENCODE lzo
	,"database" VARCHAR(250)   ENCODE lzo
	,group_type VARCHAR(30)   ENCODE lzo
	,group_territory_id NUMERIC(15,0)   ENCODE az64
	,owner_assigned_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,owner_assigned_flag VARCHAR(1)   ENCODE lzo
	,inv_platform_org_id NUMERIC(15,0)   ENCODE az64
	,component_version VARCHAR(3)   ENCODE lzo
	,subcomponent_version VARCHAR(3)   ENCODE lzo
	,comm_pref_code VARCHAR(30)   ENCODE lzo
	,last_update_channel VARCHAR(30)   ENCODE lzo
	,cust_pref_lang_code VARCHAR(4)   ENCODE lzo
 ,ERROR_CODE VARCHAR(250) ENCODE lzo
,CATEGORY_SET_ID NUMERIC(15,0) ENCODE az64
,EXTERNAL_REFERENCE VARCHAR(30) ENCODE lzo
,INCIDENT_OCCURRED_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,INCIDENT_RESOLVED_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,INC_RESPONDED_BY_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,INCIDENT_LOCATION_ID NUMERIC(15,0) ENCODE az64  
,INCIDENT_ADDRESS VARCHAR(960) ENCODE lzo
,INCIDENT_CITY VARCHAR(60) ENCODE lzo
,INCIDENT_STATE VARCHAR(60) ENCODE lzo
,INCIDENT_COUNTRY VARCHAR(60) ENCODE lzo
,INCIDENT_PROVINCE VARCHAR(60) ENCODE lzo
,INCIDENT_POSTAL_CODE VARCHAR(60) ENCODE lzo
,INCIDENT_COUNTY VARCHAR(60) ENCODE lzo
,SR_CREATION_CHANNEL VARCHAR(50) ENCODE lzo
,DEF_DEFECT_ID NUMERIC(15,0) ENCODE az64  
,DEF_DEFECT_ID2 NUMERIC(15,0) ENCODE az64  
,CREDIT_CARD_NUMBER VARCHAR(48) ENCODE lzo
,CREDIT_CARD_TYPE_CODE VARCHAR(30) ENCODE lzo
,CREDIT_CARD_EXPIRATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,CREDIT_CARD_HOLDER_FNAME VARCHAR(250) ENCODE lzo
,CREDIT_CARD_HOLDER_MNAME VARCHAR(250) ENCODE lzo
,CREDIT_CARD_HOLDER_LNAME VARCHAR(250) ENCODE lzo
,CREDIT_CARD_ID NUMERIC(15,0) ENCODE az64
,EXTERNAL_ATTRIBUTE_1 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_2 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_3 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_4 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_5 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_6 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_7 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_8 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_9 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_10 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_11 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_12 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_13 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_14 VARCHAR(150) ENCODE lzo
,EXTERNAL_ATTRIBUTE_15 VARCHAR(150) ENCODE lzo
,EXTERNAL_CONTEXT VARCHAR(30) ENCODE lzo
,LAST_UPDATE_PROGRAM_CODE VARCHAR(30) ENCODE lzo
,CREATION_PROGRAM_CODE VARCHAR(30) ENCODE lzo
,COVERAGE_TYPE VARCHAR(30) ENCODE lzo
,BILL_TO_ACCOUNT_ID NUMERIC(15,0) ENCODE az64
,SHIP_TO_ACCOUNT_ID NUMERIC(15,0) ENCODE az64
,CUSTOMER_EMAIL_ID NUMERIC(15,0) ENCODE az64
,CUSTOMER_PHONE_ID NUMERIC(15,0) ENCODE az64
,BILL_TO_PARTY_ID NUMERIC(15,0) ENCODE az64
,SHIP_TO_PARTY_ID NUMERIC(15,0) ENCODE az64
,BILL_TO_SITE_ID NUMERIC(15,0) ENCODE az64
,SHIP_TO_SITE_ID NUMERIC(15,0) ENCODE az64
,PROGRAM_LOGIN_ID NUMERIC(15,0) ENCODE az64
,STATUS_FLAG VARCHAR(3) ENCODE lzo
,PRIMARY_CONTACT_ID NUMERIC(15,0) ENCODE az64
,INCIDENT_POINT_OF_INTEREST VARCHAR(240) ENCODE lzo
,INCIDENT_CROSS_STREET VARCHAR(240) ENCODE lzo
,INCIDENT_DIRECTION_QUALIFIER VARCHAR(30) ENCODE lzo
,INCIDENT_DISTANCE_QUALIFIER VARCHAR(240) ENCODE lzo
,INCIDENT_DISTANCE_QUAL_UOM VARCHAR(30) ENCODE lzo
,INCIDENT_ADDRESS2 VARCHAR(240) ENCODE lzo
,INCIDENT_ADDRESS3 VARCHAR(240) ENCODE lzo
,INCIDENT_ADDRESS4 VARCHAR(240) ENCODE lzo
,INCIDENT_ADDRESS_STYLE VARCHAR(30) ENCODE lzo
,INCIDENT_ADDR_LINES_PHONETIC VARCHAR(560) ENCODE lzo
,INCIDENT_PO_BOX_NUMBER VARCHAR(50) ENCODE lzo
,INCIDENT_HOUSE_NUMBER VARCHAR(50) ENCODE lzo
,INCIDENT_STREET_SUFFIX VARCHAR(50) ENCODE lzo
,INCIDENT_STREET VARCHAR(150) ENCODE lzo
,INCIDENT_STREET_NUMBER VARCHAR(50) ENCODE lzo
,INCIDENT_FLOOR VARCHAR(50) ENCODE lzo
,INCIDENT_SUITE VARCHAR(50) ENCODE lzo
,INCIDENT_POSTAL_PLUS4_CODE VARCHAR(30) ENCODE lzo
,INCIDENT_POSITION VARCHAR(50) ENCODE lzo
,INCIDENT_LOCATION_DIRECTIONS VARCHAR(640) ENCODE lzo
,INCIDENT_LOCATION_DESCRIPTION VARCHAR(2000) ENCODE lzo
,INSTALL_SITE_ID NUMERIC(15,0) ENCODE az64
,OWNING_DEPARTMENT_ID NUMERIC(15,0) ENCODE az64
,ITEM_SERIAL_NUMBER VARCHAR(30) ENCODE lzo
,INCIDENT_LOCATION_TYPE VARCHAR(30) ENCODE lzo
,INCIDENT_LAST_MODIFIED_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,UNASSIGNED_INDICATOR VARCHAR(1) ENCODE lzo
,MAINT_ORGANIZATION_ID NUMERIC(15,0) ENCODE az64
,INSTRUMENT_PAYMENT_USE_ID NUMERIC(15,0) ENCODE az64
,PROJECT_TASK_ID NUMERIC(15,0) ENCODE az64
,SLA_DATE_1 TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,SLA_DATE_2 TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,SLA_DATE_3 TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,SLA_DATE_4 TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,SLA_DATE_5 TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,SLA_DATE_6 TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,SLA_DURATION_1 NUMERIC(15,2) ENCODE az64
,SLA_DURATION_2 NUMERIC(15,2) ENCODE az64
,PRICE_LIST_HEADER_ID NUMERIC(15,0) ENCODE az64
,EXPENDITURE_ORG_ID NUMERIC(15,0) ENCODE az64
,BUSINESS_PROCESS_ID NUMERIC(15,0) ENCODE az64
,ORDER_HEADER_ID NUMERIC(15,0) ENCODE az64
,REC_PROBLEM_CODE VARCHAR(50) ENCODE lzo
,REC_RESOLUTION_CODE VARCHAR(50) ENCODE lzo
,TEMPLATE_ID NUMERIC(15,0) ENCODE az64 
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CS_INCIDENTS_ALL_B (
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
,	kca_operation
,	is_deleted_flg
,	kca_seq_id,
	kca_seq_date
)
 SELECT
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
	template_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(kca_seq_id,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CS_INCIDENTS_ALL_B;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cs_incidents_all_b';

COMMIT;