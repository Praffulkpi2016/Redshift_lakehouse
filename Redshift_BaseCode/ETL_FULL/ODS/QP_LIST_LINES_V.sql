/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents full incremental approach for ODS.
	# File Version: KPI v1.0
*/
begin;
	drop table if exists bec_ods.QP_LIST_LINES_V;
	
	CREATE TABLE IF NOT EXISTS bec_ods.QP_LIST_LINES_V
	(
		list_line_id NUMERIC(15,0)   ENCODE az64
		,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,created_by NUMERIC(15,0)   ENCODE az64
		,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,last_updated_by NUMERIC(15,0)   ENCODE az64
		,last_update_login NUMERIC(15,0)   ENCODE az64
		,program_application_id NUMERIC(15,0)   ENCODE az64
		,program_id NUMERIC(15,0)   ENCODE az64
		,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,request_id NUMERIC(15,0)   ENCODE az64
		,list_header_id NUMERIC(15,0)   ENCODE az64
		,pa_list_header_id NUMERIC(15,0)   ENCODE az64
		,product_attribute_context VARCHAR(30)   ENCODE lzo
		,product_attribute VARCHAR(30)   ENCODE lzo
		,product_attr_value VARCHAR(240)   ENCODE lzo
		,product_uom_code VARCHAR(3)   ENCODE lzo
		,list_price NUMERIC(15,0)   ENCODE az64
		,percent_price NUMERIC(15,0)   ENCODE az64
		,price_by_formula_id NUMERIC(15,0)   ENCODE az64
		,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
		,primary_uom_flag VARCHAR(1)   ENCODE lzo
		,revision VARCHAR(50)   ENCODE lzo
		,revision_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,revision_reason_code VARCHAR(30)   ENCODE lzo
		,comments VARCHAR(2000)   ENCODE lzo
		,list_line_type_code VARCHAR(30)   ENCODE lzo
		,automatic_flag VARCHAR(1)   ENCODE lzo
		,modifier_level_code VARCHAR(30)   ENCODE lzo
		,list_price_uom_code VARCHAR(3)   ENCODE lzo
		,inventory_item_id NUMERIC(15,0)   ENCODE az64
		,organization_id NUMERIC(15,0)   ENCODE az64
		,related_item_id NUMERIC(15,0)   ENCODE az64
		,number_effective_periods NUMERIC(28,10)   ENCODE az64
		,effective_period_uom VARCHAR(3)   ENCODE lzo
		,arithmetic_operator VARCHAR(30)   ENCODE lzo
		,incompatibility_grp_code VARCHAR(30)   ENCODE lzo
		,list_line_no VARCHAR(30)   ENCODE lzo
		,pricing_group_sequence NUMERIC(15,0)   ENCODE az64
		,relationship_type_id NUMERIC(15,0)   ENCODE az64
		,substitution_context VARCHAR(30)   ENCODE lzo
		,substitution_attribute VARCHAR(30)   ENCODE lzo
		,substitution_value VARCHAR(240)   ENCODE lzo
		,price_break_type_code VARCHAR(30)   ENCODE lzo
		,operand NUMERIC(28,10)   ENCODE az64
		,override_flag VARCHAR(1)   ENCODE lzo
		,print_on_invoice_flag VARCHAR(1)   ENCODE lzo
		,rebate_transaction_type_code VARCHAR(30)   ENCODE lzo
		,base_qty NUMERIC(28,10)   ENCODE az64
		,base_uom_code VARCHAR(3)   ENCODE lzo
		,accrual_qty NUMERIC(28,10)   ENCODE az64
		,accrual_uom_code VARCHAR(3)   ENCODE lzo
		,estim_accrual_rate NUMERIC(28,10)   ENCODE az64
		,generate_using_formula_id NUMERIC(15,0)   ENCODE az64
		,accrual_flag VARCHAR(1)   ENCODE lzo
		,reprice_flag VARCHAR(1)   ENCODE lzo
		,pricing_phase_id NUMERIC(15,0)   ENCODE az64
		,product_precedence NUMERIC(15,0)   ENCODE az64
		,customer_item_id NUMERIC(15,0)   ENCODE az64
		,pricing_attribute_context VARCHAR(30)   ENCODE lzo
		,pricing_attribute VARCHAR(30)   ENCODE lzo
		,pricing_attr_value_from VARCHAR(240)   ENCODE lzo
		,pricing_attr_value_to VARCHAR(240)   ENCODE lzo
		,product_attr_val_disp VARCHAR(4000)   ENCODE lzo
		,product_id VARCHAR(240)   ENCODE lzo
		,pricing_attribute_id NUMERIC(15,0)   ENCODE az64
		,product_attribute_datatype VARCHAR(30)   ENCODE lzo
		,pricing_attribute_datatype VARCHAR(30)   ENCODE lzo
		,comparison_operator_code VARCHAR(30)   ENCODE lzo
		,attr_list_line_id NUMERIC(15,0)   ENCODE az64
		,attr_creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,attr_created_by NUMERIC(15,0)   ENCODE az64
		,attr_last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,attr_last_updated_by NUMERIC(15,0)   ENCODE az64
		,attr_last_update_login NUMERIC(15,0)   ENCODE az64
		,attr_program_application_id NUMERIC(15,0)   ENCODE az64
		,attr_program_id NUMERIC(15,0)   ENCODE az64
		,attr_program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,attr_request_id NUMERIC(15,0)   ENCODE az64
		,attr_context VARCHAR(30)   ENCODE lzo
		,attr_attribute1 VARCHAR(240)   ENCODE lzo
		,attr_attribute2 VARCHAR(240)   ENCODE lzo
		,attr_attribute3 VARCHAR(240)   ENCODE lzo
		,attr_attribute4 VARCHAR(240)   ENCODE lzo
		,attr_attribute5 VARCHAR(240)   ENCODE lzo
		,attr_attribute6 VARCHAR(240)   ENCODE lzo
		,attr_attribute7 VARCHAR(240)   ENCODE lzo
		,attr_attribute8 VARCHAR(240)   ENCODE lzo
		,attr_attribute9 VARCHAR(240)   ENCODE lzo
		,attr_attribute10 VARCHAR(240)   ENCODE lzo
		,attr_attribute11 VARCHAR(240)   ENCODE lzo
		,attr_attribute12 VARCHAR(240)   ENCODE lzo
		,attr_attribute13 VARCHAR(240)   ENCODE lzo
		,attr_attribute14 VARCHAR(240)   ENCODE lzo
		,attr_attribute15 VARCHAR(240)   ENCODE lzo
		,break_uom_code VARCHAR(3)   ENCODE lzo
		,break_uom_context VARCHAR(30)   ENCODE lzo
		,break_uom_attribute VARCHAR(30)   ENCODE lzo
		,accum_attribute VARCHAR(4000)   ENCODE lzo
		,continuous_price_break_flag VARCHAR(1)   ENCODE lzo
		,kca_operation VARCHAR(10)   ENCODE lzo
		,is_deleted_flg VARCHAR(2) ENCODE lzo
		,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.QP_LIST_LINES_V
	(
		list_line_id,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		last_update_login,
		program_application_id,
		program_id,
		program_update_date,
		request_id,
		list_header_id,
		pa_list_header_id,
		product_attribute_context,
		product_attribute,
		product_attr_value,
		product_uom_code,
		list_price,
		percent_price,
		price_by_formula_id,
		start_date_active,
		end_date_active,
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
		primary_uom_flag,
		revision,
		revision_date,
		revision_reason_code,
		comments,
		list_line_type_code,
		automatic_flag,
		modifier_level_code,
		list_price_uom_code,
		inventory_item_id,
		organization_id,
		related_item_id,
		number_effective_periods,
		effective_period_uom,
		arithmetic_operator,
		incompatibility_grp_code,
		list_line_no,
		pricing_group_sequence,
		relationship_type_id,
		substitution_context,
		substitution_attribute,
		substitution_value,
		price_break_type_code,
		operand,
		override_flag,
		print_on_invoice_flag,
		rebate_transaction_type_code,
		base_qty,
		base_uom_code,
		accrual_qty,
		accrual_uom_code,
		estim_accrual_rate,
		generate_using_formula_id,
		accrual_flag,
		reprice_flag,
		pricing_phase_id,
		product_precedence,
		customer_item_id,
		pricing_attribute_context,
		pricing_attribute,
		pricing_attr_value_from,
		pricing_attr_value_to,
		product_attr_val_disp,
		product_id,
		pricing_attribute_id,
		product_attribute_datatype,
		pricing_attribute_datatype,
		comparison_operator_code,
		attr_list_line_id,
		attr_creation_date,
		attr_created_by,
		attr_last_update_date,
		attr_last_updated_by,
		attr_last_update_login,
		attr_program_application_id,
		attr_program_id,
		attr_program_update_date,
		attr_request_id,
		attr_context,
		attr_attribute1,
		attr_attribute2,
		attr_attribute3,
		attr_attribute4,
		attr_attribute5,
		attr_attribute6,
		attr_attribute7,
		attr_attribute8,
		attr_attribute9,
		attr_attribute10,
		attr_attribute11,
		attr_attribute12,
		attr_attribute13,
		attr_attribute14,
		attr_attribute15,
		break_uom_code,
		break_uom_context,
		break_uom_attribute,
		accum_attribute,
		continuous_price_break_flag 
		,kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		list_line_id,
		creation_date,
		created_by,
		last_update_date,
		last_updated_by,
		last_update_login,
		program_application_id,
		program_id,
		program_update_date,
		request_id,
		list_header_id,
		pa_list_header_id,
		product_attribute_context,
		product_attribute,
		product_attr_value,
		product_uom_code,
		list_price,
		percent_price,
		price_by_formula_id,
		start_date_active,
		end_date_active,
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
		primary_uom_flag,
		revision,
		revision_date,
		revision_reason_code,
		comments,
		list_line_type_code,
		automatic_flag,
		modifier_level_code,
		list_price_uom_code,
		inventory_item_id,
		organization_id,
		related_item_id,
		number_effective_periods,
		effective_period_uom,
		arithmetic_operator,
		incompatibility_grp_code,
		list_line_no,
		pricing_group_sequence,
		relationship_type_id,
		substitution_context,
		substitution_attribute,
		substitution_value,
		price_break_type_code,
		operand,
		override_flag,
		print_on_invoice_flag,
		rebate_transaction_type_code,
		base_qty,
		base_uom_code,
		accrual_qty,
		accrual_uom_code,
		estim_accrual_rate,
		generate_using_formula_id,
		accrual_flag,
		reprice_flag,
		pricing_phase_id,
		product_precedence,
		customer_item_id,
		pricing_attribute_context,
		pricing_attribute,
		pricing_attr_value_from,
		pricing_attr_value_to,
		product_attr_val_disp,
		product_id,
		pricing_attribute_id,
		product_attribute_datatype,
		pricing_attribute_datatype,
		comparison_operator_code,
		attr_list_line_id,
		attr_creation_date,
		attr_created_by,
		attr_last_update_date,
		attr_last_updated_by,
		attr_last_update_login,
		attr_program_application_id,
		attr_program_id,
		attr_program_update_date,
		attr_request_id,
		attr_context,
		attr_attribute1,
		attr_attribute2,
		attr_attribute3,
		attr_attribute4,
		attr_attribute5,
		attr_attribute6,
		attr_attribute7,
		attr_attribute8,
		attr_attribute9,
		attr_attribute10,
		attr_attribute11,
		attr_attribute12,
		attr_attribute13,
		attr_attribute14,
		attr_attribute15,
		break_uom_code,
		break_uom_context,
		break_uom_attribute,
		accum_attribute,
		continuous_price_break_flag,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date 
		from bec_ods_stg.QP_LIST_LINES_V
	);
	
end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='qp_list_lines_v'; 

commit;