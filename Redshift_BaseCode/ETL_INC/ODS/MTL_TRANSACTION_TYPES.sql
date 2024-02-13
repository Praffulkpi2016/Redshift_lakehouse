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
	bec_ods.MTL_TRANSACTION_TYPES
where
	( NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA')  )in (
	select
		NVL(stg.TRANSACTION_TYPE_ID,0) AS TRANSACTION_TYPE_ID, NVL(stg.TRANSACTION_TYPE_NAME,'NA') AS TRANSACTION_TYPE_NAME
	from
		bec_ods.MTL_TRANSACTION_TYPES ods,
		bec_ods_stg.MTL_TRANSACTION_TYPES stg
	where
		NVL(ods.TRANSACTION_TYPE_ID,0) = NVL(stg.TRANSACTION_TYPE_ID,0) and 
		NVL(ods.TRANSACTION_TYPE_NAME,'NA') = NVL(stg.TRANSACTION_TYPE_NAME,'NA')
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MTL_TRANSACTION_TYPES
       (
    transaction_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	transaction_type_name,
	description,
	transaction_action_id,
	transaction_source_type_id,
	disable_date,
	user_defined_flag,
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
	attribute_category,
	type_class,
	shortage_msg_background_flag,
	shortage_msg_online_flag,
	status_control_flag,
	location_required_flag,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
(
	select
		transaction_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	transaction_type_name,
	description,
	transaction_action_id,
	transaction_source_type_id,
	disable_date,
	user_defined_flag,
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
	attribute_category,
	type_class,
	shortage_msg_background_flag,
	shortage_msg_online_flag,
	status_control_flag,
	location_required_flag,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MTL_TRANSACTION_TYPES
	where
		kca_operation IN ('INSERT','UPDATE')
		and (NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'),kca_seq_id) in 
	(
		select
			NVL(TRANSACTION_TYPE_ID,0) as TRANSACTION_TYPE_ID ,
			NVL(TRANSACTION_TYPE_NAME,'NA') as TRANSACTION_TYPE_NAME,max(kca_seq_id)
		from
			bec_ods_stg.MTL_TRANSACTION_TYPES
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'))
);

commit;
 
-- Soft delete
update bec_ods.MTL_TRANSACTION_TYPES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MTL_TRANSACTION_TYPES set IS_DELETED_FLG = 'Y'
where (NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'))  in
(
select NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA') from bec_raw_dl_ext.MTL_TRANSACTION_TYPES
where (NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'),KCA_SEQ_ID)
in 
(
select NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MTL_TRANSACTION_TYPES
group by NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA')
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
	ods_table_name = 'mtl_transaction_types';

commit;