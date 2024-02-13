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

delete from bec_dwh.DIM_INV_TXN_SOURCE_TYPES
where (nvl(TRANSACTION_SOURCE_TYPE_ID,0)) in (
select nvl(ods.TRANSACTION_SOURCE_TYPE_ID,0) from bec_dwh.DIM_INV_TXN_SOURCE_TYPES dw, bec_ods.MTL_TXN_SOURCE_TYPES ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.TRANSACTION_SOURCE_TYPE_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_inv_txn_source_types' and batch_name = 'inv')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_INV_TXN_SOURCE_TYPES
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
	transaction_source
	,is_deleted_flg
	,source_app_id
	,dw_load_id
	,dw_insert_date
	,dw_update_date
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
 where 1=1
 and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_inv_txn_source_types' and batch_name = 'inv')
 )
 );

-- Soft delete

update bec_dwh.DIM_INV_TXN_SOURCE_TYPES set is_deleted_flg = 'Y'
where (nvl(TRANSACTION_SOURCE_TYPE_ID,0)) not in (
select nvl(ods.TRANSACTION_SOURCE_TYPE_ID,0) as TRANSACTION_SOURCE_TYPE_ID from bec_dwh.DIM_INV_TXN_SOURCE_TYPES dw, bec_ods.MTL_TXN_SOURCE_TYPES ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'|| nvl(ods.TRANSACTION_SOURCE_TYPE_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inv_txn_source_types' and batch_name = 'inv';

COMMIT;