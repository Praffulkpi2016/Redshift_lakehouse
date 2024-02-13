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

truncate
	table bec_ods_stg.MTL_TRANSACTION_TYPES;

insert
	into
	bec_ods_stg.MTL_TRANSACTION_TYPES
(transaction_type_id,
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
	KCA_OPERATION,
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
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.MTL_TRANSACTION_TYPES
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
and (NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'),kca_seq_id,last_update_date) in 
(select NVL(TRANSACTION_TYPE_ID,0) AS TRANSACTION_TYPE_ID,NVL(TRANSACTION_TYPE_NAME,'NA') AS TRANSACTION_TYPE_NAME,max(kca_seq_id),max(last_update_date) from bec_raw_dl_ext.MTL_TRANSACTION_TYPES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by NVL(TRANSACTION_TYPE_ID,0) ,NVL(TRANSACTION_TYPE_NAME,'NA'))
and 
( kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'mtl_transaction_types')
 
            )
);		
		
end;