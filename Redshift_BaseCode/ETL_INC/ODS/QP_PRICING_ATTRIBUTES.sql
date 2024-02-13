/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
-- Delete Records

delete
from
	bec_ods.QP_PRICING_ATTRIBUTES
where
	(
	nvl(PRICING_ATTRIBUTE_ID, 0) 
	) in 
	(
	select
		NVL(stg.PRICING_ATTRIBUTE_ID, 0) as PRICING_ATTRIBUTE_ID 
	from
		bec_ods.QP_PRICING_ATTRIBUTES ods,
		bec_ods_stg.QP_PRICING_ATTRIBUTES stg
	where
		    NVL(ods.PRICING_ATTRIBUTE_ID, 0) = NVL(stg.PRICING_ATTRIBUTE_ID, 0) 
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

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
	,kca_seq_date)
(
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
		,kca_seq_date
	from
		bec_ods_stg.QP_PRICING_ATTRIBUTES
	where
		kca_operation in ('INSERT','UPDATE')
		and (
		nvl(PRICING_ATTRIBUTE_ID, 0) ,
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(PRICING_ATTRIBUTE_ID, 0) as PRICING_ATTRIBUTE_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.QP_PRICING_ATTRIBUTES
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			PRICING_ATTRIBUTE_ID 
			)	
	);

commit;

-- Soft delete
update bec_ods.QP_PRICING_ATTRIBUTES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.QP_PRICING_ATTRIBUTES set IS_DELETED_FLG = 'Y'
where (PRICING_ATTRIBUTE_ID)  in
(
select PRICING_ATTRIBUTE_ID from bec_raw_dl_ext.QP_PRICING_ATTRIBUTES
where (PRICING_ATTRIBUTE_ID,KCA_SEQ_ID)
in 
(
select PRICING_ATTRIBUTE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.QP_PRICING_ATTRIBUTES
group by PRICING_ATTRIBUTE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'qp_pricing_attributes';

commit;