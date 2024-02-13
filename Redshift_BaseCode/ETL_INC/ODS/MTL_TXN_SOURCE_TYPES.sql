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
	bec_ods.MTL_TXN_SOURCE_TYPES
where
	TRANSACTION_SOURCE_TYPE_ID in (
	select
		stg.TRANSACTION_SOURCE_TYPE_ID
	from
		bec_ods.MTL_TXN_SOURCE_TYPES ods,
		bec_ods_stg.MTL_TXN_SOURCE_TYPES stg
	where
		ods.TRANSACTION_SOURCE_TYPE_ID = stg.TRANSACTION_SOURCE_TYPE_ID
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_TXN_SOURCE_TYPES
       (
    transaction_source_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	transaction_source_type_name,
	description,
	disable_date,
	user_defined_flag,
	validated_flag,
	descriptive_flex_context_code,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	transaction_source_category,
	transaction_source,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
(
	select
		transaction_source_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	transaction_source_type_name,
	description,
	disable_date,
	user_defined_flag,
	validated_flag,
	descriptive_flex_context_code,
	attribute_category,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	transaction_source_category,
	transaction_source,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
	from
		bec_ods_stg.MTL_TXN_SOURCE_TYPES
	where
		kca_operation in ('INSERT','UPDATE')
		and (TRANSACTION_SOURCE_TYPE_ID,kca_seq_id) in 
	(
		select
			TRANSACTION_SOURCE_TYPE_ID,max(kca_seq_id)
		from
			bec_ods_stg.MTL_TXN_SOURCE_TYPES
		where
			kca_operation in ('INSERT','UPDATE')
		group by
			TRANSACTION_SOURCE_TYPE_ID)
);

commit;

-- Soft delete
update bec_ods.MTL_TXN_SOURCE_TYPES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_TXN_SOURCE_TYPES set IS_DELETED_FLG = 'Y'
where (TRANSACTION_SOURCE_TYPE_ID)  in
(
select TRANSACTION_SOURCE_TYPE_ID from bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
where (TRANSACTION_SOURCE_TYPE_ID,KCA_SEQ_ID)
in 
(
select TRANSACTION_SOURCE_TYPE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
group by TRANSACTION_SOURCE_TYPE_ID
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
	ods_table_name = 'mtl_txn_source_types';

commit;