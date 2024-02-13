/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.DIM_INV_TRANSACTION_TYPES;

create table bec_dwh.DIM_INV_TRANSACTION_TYPES 
diststyle all 
sortkey(transaction_type_id) as
(select
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
	from bec_ods.mtl_transaction_types);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_inv_transaction_types'
	and batch_name = 'inv';

commit;