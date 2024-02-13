/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/


begin;
-- Delete Records

delete
from
	bec_dwh.DIM_INV_TRANSACTION_TYPES
where
	(nvl(transaction_type_id,0),
	nvl(transaction_type_name,'NA')) in
(
	select
		nvl(ods.transaction_type_id,0),
		nvl(ods.transaction_type_name,'NA')
	from
		bec_dwh.DIM_INV_TRANSACTION_TYPES dw,
		bec_ods.mtl_transaction_types ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.transaction_type_id, 0)
|| '-' || nvl(ods.transaction_type_name, 'NA')
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_inv_transaction_types'
				and batch_name = 'inv')
				 )
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_INV_TRANSACTION_TYPES
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
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
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
		-- audit columns
	'N' as is_deleted_flg,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS') as source_app_id,
		(
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
	|| '-' || nvl(transaction_type_id, 0) || '-' || nvl(transaction_type_name, 'NA') as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.mtl_transaction_types
	where
1=1
			and (kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_inv_transaction_types'
				and batch_name = 'inv')
				 )
 );
-- Soft delete

update
	bec_dwh.DIM_INV_TRANSACTION_TYPES
set
	is_deleted_flg = 'Y'
where
	(nvl(transaction_type_id,0),
		nvl(transaction_type_name,'NA')) not in (
	select
		nvl(ods.transaction_type_id,0),
		nvl(ods.transaction_type_name,'NA')
	from
		bec_dwh.DIM_INV_TRANSACTION_TYPES dw,
		(select * from bec_ods.mtl_transaction_types where is_deleted_flg <> 'Y')ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.transaction_type_id, 0)
|| '-' || nvl(ods.transaction_type_name, 'NA')
);

commit;
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_transaction_types' and batch_name = 'inv';

COMMIT;