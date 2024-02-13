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
DELETE FROM bec_dwh.dim_cost_types
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.cst_cost_types ods
    WHERE NVL(dim_cost_types.cost_type_id, 0) = NVL(ods.cost_type_id, 0)
    AND ods.kca_seq_date > (
        SELECT (executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_cost_types'
        AND batch_name = 'costing'
    )
);
commit;
-- Insert records

insert
	into
	bec_dwh.dim_cost_types
(
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	organization_id,
	cost_type,
	description,
	costing_method_type,
	frozen_standard_flag,
	default_cost_type_id,
	bom_snapshot_flag,
	alternate_bom_designator,
	allow_updates_flag,
	pl_element_flag,
	pl_resource_flag,
	pl_operation_flag,
	pl_activity_flag,
	disable_date,
	available_to_eng_flag,
	component_yield_flag,
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
	zd_edition_name,
	zd_sync,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		cost_type_id,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		organization_id,
		cost_type,
		description,
		costing_method_type,
		frozen_standard_flag,
		default_cost_type_id,
		bom_snapshot_flag,
		alternate_bom_designator,
		allow_updates_flag,
		pl_element_flag,
		pl_resource_flag,
		pl_operation_flag,
		pl_activity_flag,
		disable_date,
		available_to_eng_flag,
		component_yield_flag,
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
		zd_edition_name,
		zd_sync,
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
	|| '-' || nvl(cost_type_id, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.cst_cost_types
	where
			(kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_cost_types'
				and batch_name = 'costing')	
				 	)

);
-- Soft delete
update bec_dwh.dim_cost_types set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
    SELECT 1
    FROM bec_ods.cst_cost_types ods
    WHERE NVL(dim_cost_types.cost_type_id, 0) = NVL(ods.cost_type_id, 0)
    AND ods.is_deleted_flg <> 'Y'
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_cost_types'
	and batch_name = 'costing';

commit;