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

DROP TABLE if exists bec_ods.QP_PRICING_ATTRIBUTES;

CREATE TABLE IF NOT EXISTS bec_ods.QP_PRICING_ATTRIBUTES
(

     pricing_attribute_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,list_line_id NUMERIC(15,0)   ENCODE az64
	,excluder_flag VARCHAR(1)   ENCODE lzo
	,accumulate_flag VARCHAR(1)   ENCODE lzo
	,product_attribute_context VARCHAR(30)   ENCODE lzo
	,product_attribute VARCHAR(30)   ENCODE lzo
	,product_attr_value VARCHAR(240)   ENCODE lzo
	,product_uom_code VARCHAR(3)   ENCODE lzo
	,pricing_attribute_context VARCHAR(30)   ENCODE lzo
	,pricing_attribute VARCHAR(30)   ENCODE lzo
	,pricing_attr_value_from VARCHAR(240)   ENCODE lzo
	,pricing_attr_value_to VARCHAR(240)   ENCODE lzo
	,attribute_grouping_no NUMERIC(15,0)   ENCODE az64
	,product_attribute_datatype VARCHAR(30)   ENCODE lzo
	,pricing_attribute_datatype VARCHAR(30)   ENCODE lzo
	,comparison_operator_code VARCHAR(30)   ENCODE lzo
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
	,list_header_id NUMERIC(15,0)   ENCODE az64
	,pricing_phase_id NUMERIC(15,0)   ENCODE az64
	,qualification_ind NUMERIC(15,0)   ENCODE az64
	,pricing_attr_value_from_number NUMERIC(28,10)   ENCODE az64
	,pricing_attr_value_to_number NUMERIC(28,10)   ENCODE az64
	,distinct_row_count NUMERIC(28,10)   ENCODE az64
	,search_ind NUMERIC(28,10)   ENCODE az64
	,pattern_value_from_positive VARCHAR(240)   ENCODE lzo
	,pattern_value_to_positive VARCHAR(240)   ENCODE lzo
	,pattern_value_from_negative VARCHAR(240)   ENCODE lzo
	,pattern_value_to_negative VARCHAR(240)   ENCODE lzo
	,product_segment_id NUMERIC(15,0)   ENCODE az64
	,pricing_segment_id NUMERIC(15,0)   ENCODE az64
	,orig_sys_line_ref VARCHAR(50)   ENCODE lzo
	,orig_sys_pricing_attr_ref VARCHAR(50)   ENCODE lzo
	,orig_sys_header_ref VARCHAR(50)   ENCODE lzo 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.QP_PRICING_ATTRIBUTES (
   pricing_attribute_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_line_id,
	excluder_flag,
	accumulate_flag,
	product_attribute_context,
	product_attribute,
	product_attr_value,
	product_uom_code,
	pricing_attribute_context,
	pricing_attribute,
	pricing_attr_value_from,
	pricing_attr_value_to,
	attribute_grouping_no,
	product_attribute_datatype,
	pricing_attribute_datatype,
	comparison_operator_code,
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
	list_header_id,
	pricing_phase_id,
	qualification_ind,
	pricing_attr_value_from_number,
	pricing_attr_value_to_number,
	distinct_row_count,
	search_ind,
	pattern_value_from_positive,
	pattern_value_to_positive,
	pattern_value_from_negative,
	pattern_value_to_negative,
	product_segment_id,
	pricing_segment_id,
	orig_sys_line_ref,
	orig_sys_pricing_attr_ref,
	orig_sys_header_ref,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    select
	pricing_attribute_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_line_id,
	excluder_flag,
	accumulate_flag,
	product_attribute_context,
	product_attribute,
	product_attr_value,
	product_uom_code,
	pricing_attribute_context,
	pricing_attribute,
	pricing_attr_value_from,
	pricing_attr_value_to,
	attribute_grouping_no,
	product_attribute_datatype,
	pricing_attribute_datatype,
	comparison_operator_code,
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
	list_header_id,
	pricing_phase_id,
	qualification_ind,
	pricing_attr_value_from_number,
	pricing_attr_value_to_number,
	distinct_row_count,
	search_ind,
	pattern_value_from_positive,
	pattern_value_to_positive,
	pattern_value_from_negative,
	pattern_value_to_negative,
	product_segment_id,
	pricing_segment_id,
	orig_sys_line_ref,
	orig_sys_pricing_attr_ref,
	orig_sys_header_ref,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.QP_PRICING_ATTRIBUTES;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'qp_pricing_attributes';

commit;