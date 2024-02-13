/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS. .
# File Version: KPI v1.0
*/

begin;
DROP TABLE if exists bec_ods.OE_ORDER_HEADERS_ALL;
CREATE TABLE IF NOT EXISTS bec_ods.OE_ORDER_HEADERS_ALL
(
	header_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,order_type_id NUMERIC(15,0)   ENCODE az64
	,order_number NUMERIC(15,0)   ENCODE az64
	,version_number NUMERIC(15,0)   ENCODE az64
	,expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_source_id NUMERIC(15,0)   ENCODE az64
	,source_document_type_id NUMERIC(15,0)   ENCODE az64
	,orig_sys_document_ref VARCHAR(240)   ENCODE lzo
	,source_document_id NUMERIC(15,0)   ENCODE az64
	,ordered_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,pricing_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_priority_code VARCHAR(30)   ENCODE lzo
	,demand_class_code VARCHAR(30)   ENCODE lzo
	,price_list_id NUMERIC(15,0)   ENCODE az64
	,tax_exempt_flag VARCHAR(1)   ENCODE lzo
	,tax_exempt_number VARCHAR(80)   ENCODE lzo
	,tax_exempt_reason_code VARCHAR(30)   ENCODE lzo
	,conversion_rate NUMERIC(28,10)   ENCODE az64
	,conversion_type_code VARCHAR(30)   ENCODE lzo
	,conversion_rate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,partial_shipments_allowed VARCHAR(1)   ENCODE lzo
	,ship_tolerance_above NUMERIC(28,10)   ENCODE az64
	,ship_tolerance_below NUMERIC(28,10)   ENCODE az64
	,transactional_curr_code VARCHAR(15)   ENCODE lzo
	,agreement_id NUMERIC(15,0)   ENCODE az64
	,tax_point_code VARCHAR(30)   ENCODE lzo
	,cust_po_number VARCHAR(50)   ENCODE lzo
	,invoicing_rule_id NUMERIC(15,0)   ENCODE az64
	,accounting_rule_id NUMERIC(15,0)   ENCODE az64
	,payment_term_id NUMERIC(15,0)   ENCODE az64
	,shipping_method_code VARCHAR(30)   ENCODE lzo
	,freight_carrier_code VARCHAR(30)   ENCODE lzo
	,fob_point_code VARCHAR(30)   ENCODE lzo
	,freight_terms_code VARCHAR(30)   ENCODE lzo
	,sold_from_org_id NUMERIC(15,0)   ENCODE az64
	,sold_to_org_id NUMERIC(15,0)   ENCODE az64
	,ship_from_org_id NUMERIC(15,0)   ENCODE az64
	,ship_to_org_id NUMERIC(15,0)   ENCODE az64
	,invoice_to_org_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_org_id NUMERIC(15,0)   ENCODE az64
	,sold_to_contact_id NUMERIC(15,0)   ENCODE az64
	,ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,invoice_to_contact_id NUMERIC(15,0)   ENCODE az64
	,deliver_to_contact_id NUMERIC(36,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,context VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(240)   ENCODE lzo
	,attribute2 VARCHAR(240)   ENCODE lzo
	,attribute3 VARCHAR(240)   ENCODE lzo
	,attribute4 VARCHAR(240)   ENCODE lzo
	,attribute5 VARCHAR(240)   ENCODE lzo
	,attribute6 VARCHAR(240)   ENCODE lzo
	,attribute7 VARCHAR(240)   ENCODE lzo
	,attribute8 VARCHAR(240)   ENCODE lzo
	,attribute9 VARCHAR(240)   ENCODE lzo
	,attribute10 VARCHAR(240)   ENCODE lzo
	,attribute11 VARCHAR(240)   ENCODE lzo
	,attribute12 VARCHAR(240)   ENCODE lzo
	,attribute13 VARCHAR(240)   ENCODE lzo
	,attribute14 VARCHAR(240)   ENCODE lzo
	,attribute15 VARCHAR(240)   ENCODE lzo
	,global_attribute_category VARCHAR(240)   ENCODE lzo
	,global_attribute1 VARCHAR(240)   ENCODE lzo
	,global_attribute2 VARCHAR(240)   ENCODE lzo
	,global_attribute3 VARCHAR(240)   ENCODE lzo
	,global_attribute4 VARCHAR(240)   ENCODE lzo
	,global_attribute5 VARCHAR(240)   ENCODE lzo
	,global_attribute6 VARCHAR(240)   ENCODE lzo
	,global_attribute7 VARCHAR(240)   ENCODE lzo
	,global_attribute8 VARCHAR(240)   ENCODE lzo
	,global_attribute9 VARCHAR(240)   ENCODE lzo
	,global_attribute10 VARCHAR(240)   ENCODE lzo
	,global_attribute11 VARCHAR(240)   ENCODE lzo
	,global_attribute12 VARCHAR(240)   ENCODE lzo
	,global_attribute13 VARCHAR(240)   ENCODE lzo
	,global_attribute14 VARCHAR(240)   ENCODE lzo
	,global_attribute15 VARCHAR(240)   ENCODE lzo
	,global_attribute16 VARCHAR(240)   ENCODE lzo
	,global_attribute17 VARCHAR(240)   ENCODE lzo
	,global_attribute18 VARCHAR(240)   ENCODE lzo
	,global_attribute19 VARCHAR(240)   ENCODE lzo
	,global_attribute20 VARCHAR(240)   ENCODE lzo
	,cancelled_flag VARCHAR(1)   ENCODE lzo
	,open_flag VARCHAR(1)   ENCODE lzo
	,booked_flag VARCHAR(1)   ENCODE lzo
	,salesrep_id NUMERIC(15,0)   ENCODE az64
	,return_reason_code VARCHAR(30)   ENCODE lzo
	,order_date_type_code VARCHAR(30)   ENCODE lzo
	,earliest_schedule_limit NUMERIC(15,0)   ENCODE az64
	,latest_schedule_limit NUMERIC(15,0)   ENCODE az64
	,payment_type_code VARCHAR(30)   ENCODE lzo
	,payment_amount NUMERIC(28,10)   ENCODE az64
	,check_number VARCHAR(50)   ENCODE lzo
	,credit_card_code VARCHAR(80)   ENCODE lzo
	,credit_card_holder_name VARCHAR(80)   ENCODE lzo
	,credit_card_number VARCHAR(80)   ENCODE lzo
	,credit_card_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,credit_card_approval_code VARCHAR(80)   ENCODE lzo
	,sales_channel_code VARCHAR(30)   ENCODE lzo
	,first_ack_code VARCHAR(30)   ENCODE lzo
	,first_ack_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_ack_code VARCHAR(30)   ENCODE lzo
	,last_ack_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_category_code VARCHAR(30)   ENCODE lzo
	,change_sequence VARCHAR(50)   ENCODE lzo
	,drop_ship_flag VARCHAR(1)   ENCODE lzo
	,customer_payment_term_id NUMERIC(15,0)   ENCODE az64
	,shipping_instructions VARCHAR(2000)   ENCODE lzo
	,packing_instructions VARCHAR(2000)   ENCODE lzo
	,tp_context VARCHAR(30)   ENCODE lzo
	,tp_attribute1 VARCHAR(240)   ENCODE lzo
	,tp_attribute2 VARCHAR(240)   ENCODE lzo
	,tp_attribute3 VARCHAR(240)   ENCODE lzo
	,tp_attribute4 VARCHAR(240)   ENCODE lzo
	,tp_attribute5 VARCHAR(240)   ENCODE lzo
	,tp_attribute6 VARCHAR(240)   ENCODE lzo
	,tp_attribute7 VARCHAR(240)   ENCODE lzo
	,tp_attribute8 VARCHAR(240)   ENCODE lzo
	,tp_attribute9 VARCHAR(240)   ENCODE lzo
	,tp_attribute10 VARCHAR(240)   ENCODE lzo
	,tp_attribute11 VARCHAR(240)   ENCODE lzo
	,tp_attribute12 VARCHAR(240)   ENCODE lzo
	,tp_attribute13 VARCHAR(240)   ENCODE lzo
	,tp_attribute14 VARCHAR(240)   ENCODE lzo
	,tp_attribute15 VARCHAR(240)   ENCODE lzo
	,flow_status_code VARCHAR(30)   ENCODE lzo
	,marketing_source_code_id NUMERIC(15,0)   ENCODE az64
	,credit_card_approval_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,upgraded_flag VARCHAR(1)   ENCODE lzo
	,customer_preference_set_code VARCHAR(30)   ENCODE lzo
	,booked_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,lock_control NUMERIC(15,0)   ENCODE az64
	,price_request_code VARCHAR(240)   ENCODE lzo
	,batch_id NUMERIC(15,0)   ENCODE az64
	,xml_message_id NUMERIC(15,0)   ENCODE az64
	,accounting_rule_duration NUMERIC(15,0)   ENCODE az64
	,attribute16 VARCHAR(240)   ENCODE lzo
	,attribute17 VARCHAR(240)   ENCODE lzo
	,attribute18 VARCHAR(240)   ENCODE lzo
	,attribute19 VARCHAR(240)   ENCODE lzo
	,attribute20 VARCHAR(240)   ENCODE lzo
	,blanket_number NUMERIC(15,0)   ENCODE az64
	,sales_document_type_code VARCHAR(30)   ENCODE lzo
	,sold_to_phone_id NUMERIC(15,0)   ENCODE az64
	,fulfillment_set_name VARCHAR(30)   ENCODE lzo
	,line_set_name VARCHAR(30)   ENCODE lzo
	,default_fulfillment_set VARCHAR(1)   ENCODE lzo
	,transaction_phase_code VARCHAR(30)   ENCODE lzo
	,sales_document_name VARCHAR(240)   ENCODE lzo
	,quote_number NUMERIC(15,0)   ENCODE az64
	,quote_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,user_status_code VARCHAR(30)   ENCODE lzo
	,draft_submitted_flag VARCHAR(1)   ENCODE lzo
	,source_document_version_number NUMERIC(15,0)   ENCODE az64
	,sold_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,supplier_signature VARCHAR(240)   ENCODE lzo
	,supplier_signature_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,customer_signature VARCHAR(240)   ENCODE lzo
	,customer_signature_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,minisite_id NUMERIC(15,0)   ENCODE az64
	,end_customer_id NUMERIC(15,0)   ENCODE az64
	,end_customer_contact_id NUMERIC(15,0)   ENCODE az64
	,end_customer_site_use_id NUMERIC(15,0)   ENCODE az64
	,ib_owner VARCHAR(60)   ENCODE lzo
	,ib_current_location VARCHAR(60)   ENCODE lzo
	,ib_installed_at_location VARCHAR(60)   ENCODE lzo
	,order_firmed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inst_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OE_ORDER_HEADERS_ALL (
    header_id,
	org_id,
	order_type_id,
	order_number,
	version_number,
	expiration_date,
	order_source_id,
	source_document_type_id,
	orig_sys_document_ref,
	source_document_id,
	ordered_date,
	request_date,
	pricing_date,
	shipment_priority_code,
	demand_class_code,
	price_list_id,
	tax_exempt_flag,
	tax_exempt_number,
	tax_exempt_reason_code,
	conversion_rate,
	conversion_type_code,
	conversion_rate_date,
	partial_shipments_allowed,
	ship_tolerance_above,
	ship_tolerance_below,
	transactional_curr_code,
	agreement_id,
	tax_point_code,
	cust_po_number,
	invoicing_rule_id,
	accounting_rule_id,
	payment_term_id,
	shipping_method_code,
	freight_carrier_code,
	fob_point_code,
	freight_terms_code,
	sold_from_org_id,
	sold_to_org_id,
	ship_from_org_id,
	ship_to_org_id,
	invoice_to_org_id,
	deliver_to_org_id,
	sold_to_contact_id,
	ship_to_contact_id,
	invoice_to_contact_id,
	deliver_to_contact_id,
	creation_date,
	created_by,
	last_updated_by,
	last_update_date,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
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
	cancelled_flag,
	open_flag,
	booked_flag,
	salesrep_id,
	return_reason_code,
	order_date_type_code,
	earliest_schedule_limit,
	latest_schedule_limit,
	payment_type_code,
	payment_amount,
	check_number,
	credit_card_code,
	credit_card_holder_name,
	credit_card_number,
	credit_card_expiration_date,
	credit_card_approval_code,
	sales_channel_code,
	first_ack_code,
	first_ack_date,
	last_ack_code,
	last_ack_date,
	order_category_code,
	change_sequence,
	drop_ship_flag,
	customer_payment_term_id,
	shipping_instructions,
	packing_instructions,
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
	flow_status_code,
	marketing_source_code_id,
	credit_card_approval_date,
	upgraded_flag,
	customer_preference_set_code,
	booked_date,
	lock_control,
	price_request_code,
	batch_id,
	xml_message_id,
	accounting_rule_duration,
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
	blanket_number,
	sales_document_type_code,
	sold_to_phone_id,
	fulfillment_set_name,
	line_set_name,
	default_fulfillment_set,
	transaction_phase_code,
	sales_document_name,
	quote_number,
	quote_date,
	user_status_code,
	draft_submitted_flag,
	source_document_version_number,
	sold_to_site_use_id,
	supplier_signature,
	supplier_signature_date,
	customer_signature,
	customer_signature_date,
	minisite_id,
	end_customer_id,
	end_customer_contact_id,
	end_customer_site_use_id,
	ib_owner,
	ib_current_location,
	ib_installed_at_location,
	order_firmed_date,
	inst_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
    header_id,
	org_id,
	order_type_id,
	order_number,
	version_number,
	expiration_date,
	order_source_id,
	source_document_type_id,
	orig_sys_document_ref,
	source_document_id,
	ordered_date,
	request_date,
	pricing_date,
	shipment_priority_code,
	demand_class_code,
	price_list_id,
	tax_exempt_flag,
	tax_exempt_number,
	tax_exempt_reason_code,
	conversion_rate,
	conversion_type_code,
	conversion_rate_date,
	partial_shipments_allowed,
	ship_tolerance_above,
	ship_tolerance_below,
	transactional_curr_code,
	agreement_id,
	tax_point_code,
	cust_po_number,
	invoicing_rule_id,
	accounting_rule_id,
	payment_term_id,
	shipping_method_code,
	freight_carrier_code,
	fob_point_code,
	freight_terms_code,
	sold_from_org_id,
	sold_to_org_id,
	ship_from_org_id,
	ship_to_org_id,
	invoice_to_org_id,
	deliver_to_org_id,
	sold_to_contact_id,
	ship_to_contact_id,
	invoice_to_contact_id,
	deliver_to_contact_id,
	creation_date,
	created_by,
	last_updated_by,
	last_update_date,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
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
	cancelled_flag,
	open_flag,
	booked_flag,
	salesrep_id,
	return_reason_code,
	order_date_type_code,
	earliest_schedule_limit,
	latest_schedule_limit,
	payment_type_code,
	payment_amount,
	check_number,
	credit_card_code,
	credit_card_holder_name,
	credit_card_number,
	credit_card_expiration_date,
	credit_card_approval_code,
	sales_channel_code,
	first_ack_code,
	first_ack_date,
	last_ack_code,
	last_ack_date,
	order_category_code,
	change_sequence,
	drop_ship_flag,
	customer_payment_term_id,
	shipping_instructions,
	packing_instructions,
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
	flow_status_code,
	marketing_source_code_id,
	credit_card_approval_date,
	upgraded_flag,
	customer_preference_set_code,
	booked_date,
	lock_control,
	price_request_code,
	batch_id,
	xml_message_id,
	accounting_rule_duration,
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
	blanket_number,
	sales_document_type_code,
	sold_to_phone_id,
	fulfillment_set_name,
	line_set_name,
	default_fulfillment_set,
	transaction_phase_code,
	sales_document_name,
	quote_number,
	quote_date,
	user_status_code,
	draft_submitted_flag,
	source_document_version_number,
	sold_to_site_use_id,
	supplier_signature,
	supplier_signature_date,
	customer_signature,
	customer_signature_date,
	minisite_id,
	end_customer_id,
	end_customer_contact_id,
	end_customer_site_use_id,
	ib_owner,
	ib_current_location,
	ib_installed_at_location,
	order_firmed_date,
	inst_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.OE_ORDER_HEADERS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oe_order_headers_all';
	
commit;