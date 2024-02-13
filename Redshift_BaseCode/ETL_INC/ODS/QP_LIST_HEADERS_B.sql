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

delete from bec_ods.QP_LIST_HEADERS_B
where NVL(LIST_HEADER_ID,0) in (
select NVL(stg.LIST_HEADER_ID,0) as LIST_HEADER_ID
from bec_ods.QP_LIST_HEADERS_B ods, bec_ods_stg.QP_LIST_HEADERS_B stg
where ods.LIST_HEADER_ID = stg.LIST_HEADER_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.QP_LIST_HEADERS_B
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
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
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
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
  KCA_SEQ_DATE
from bec_ods_stg.QP_LIST_HEADERS_B
where kca_operation in ('INSERT','UPDATE') 
	and (nvl(LIST_HEADER_ID,0),kca_seq_id) in 
	(select nvl(LIST_HEADER_ID,0) as LIST_HEADER_ID,max(kca_seq_id) from bec_ods_stg.QP_LIST_HEADERS_B 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(LIST_HEADER_ID,0))
);

commit;


-- Soft delete
update bec_ods.QP_LIST_HEADERS_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.QP_LIST_HEADERS_B set IS_DELETED_FLG = 'Y'
where (LIST_HEADER_ID)  in
(
select LIST_HEADER_ID from bec_raw_dl_ext.QP_LIST_HEADERS_B
where (LIST_HEADER_ID,KCA_SEQ_ID)
in 
(
select LIST_HEADER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.QP_LIST_HEADERS_B
group by LIST_HEADER_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate() 
where ods_table_name='qp_list_headers_b';
commit;