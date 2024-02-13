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

drop table if exists bec_dwh.DIM_INV_TXN_SOURCE_TYPES;

create table bec_dwh.DIM_INV_TXN_SOURCE_TYPES diststyle all sortkey(TRANSACTION_SOURCE_TYPE_ID)
as
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
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
       || nvl(TRANSACTION_SOURCE_TYPE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.mtl_txn_source_types
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_inv_txn_source_types'
	and batch_name = 'inv';

commit;
