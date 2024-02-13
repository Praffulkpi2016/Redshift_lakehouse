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
TRUNCATE TABLE bec_ods_stg.QP_LIST_HEADERS_B;


insert into bec_ods_stg.QP_LIST_HEADERS_B
(
	list_header_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_type_code,
	start_date_active,
	end_date_active,
	automatic_flag,
	currency_code,
	rounding_factor,
	ship_method_code,
	freight_terms_code,
	terms_id,
	comments,
	discount_lines_flag,
	gsa_indicator,
	prorate_flag,
	source_system_code,
	ask_for_flag,
	active_flag,
	parent_list_header_id,
	start_date_active_first,
	end_date_active_first,
	active_date_first_type,
	start_date_active_second,
	end_date_active_second,
	active_date_second_type,
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
	limit_exists_flag,
	mobile_download,
	currency_header_id,
	pte_code,
	list_source_code,
	orig_system_header_ref,
	orig_org_id,
	global_flag,
	shareable_flag,
	sold_to_org_id,
	locked_from_list_header_id
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
) (SELECT 
	list_header_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	list_type_code,
	start_date_active,
	end_date_active,
	automatic_flag,
	currency_code,
	rounding_factor,
	ship_method_code,
	freight_terms_code,
	terms_id,
	comments,
	discount_lines_flag,
	gsa_indicator,
	prorate_flag,
	source_system_code,
	ask_for_flag,
	active_flag,
	parent_list_header_id,
	start_date_active_first,
	end_date_active_first,
	active_date_first_type,
	start_date_active_second,
	end_date_active_second,
	active_date_second_type,
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
	limit_exists_flag,
	mobile_download,
	currency_header_id,
	pte_code,
	list_source_code,
	orig_system_header_ref,
	orig_org_id,
	global_flag,
	shareable_flag,
	sold_to_org_id,
	locked_from_list_header_id
,KCA_OPERATION
,KCA_SEQ_ID
,KCA_SEQ_DATE
 from bec_raw_dl_ext.QP_LIST_HEADERS_B 
 where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (LIST_HEADER_ID,KCA_SEQ_ID) in 
	(select LIST_HEADER_ID,max(KCA_SEQ_ID) from bec_raw_dl_ext.QP_LIST_HEADERS_B 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by LIST_HEADER_ID)
     and  (KCA_SEQ_DATE > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='qp_list_headers_b')
	)
);
END;