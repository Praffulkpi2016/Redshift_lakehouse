/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
-- Delete Records
DELETE FROM bec_dwh.dim_lookups
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.fnd_lookup_values ods
    WHERE NVL(dim_lookups.lookup_type, 'NA') = NVL(ods.lookup_type, 'NA')
    AND NVL(dim_lookups.lookup_code, 'NA') = NVL(ods.lookup_code, 'NA')
    AND NVL(dim_lookups.language, 'NA') = NVL(ods.language, 'NA')
    AND NVL(dim_lookups.view_application_id, 0) = NVL(ods.view_application_id, 0)
    AND ods.language = 'US'
    AND ods.enabled_flag = 'Y'
    AND (ods.kca_seq_date > (
            SELECT (executebegints - prune_days)
            FROM bec_etl_ctrl.batch_dw_info
            WHERE dw_table_name = 'dim_lookups'
            AND batch_name = 'ap'))
);
commit;
-- Insert records
insert
	into
	bec_dwh.dim_lookups
(
lookup_code,
	description,
	lookup_type,
	language,
	meaning,
	enabled_flag,
	view_application_id,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select distinct 
		ods.lookup_code as lookup_code,
		ods.description as description,
		ods.lookup_type as lookup_type,
		ods.language as language,
		ods.meaning as meaning,
		ods.enabled_flag as enabled_flag,
		ods.view_application_id as view_application_id,
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
		|| '-' || nvl(ods.lookup_type, 'NA')
		|| '-' || nvl(ods.lookup_code, 'NA')
		|| '-' || nvl(ods.language, 'NA')
		|| '-' || nvl(ods.view_application_id, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.fnd_lookup_values ods
	where
		1 = 1
		and ods.language = 'US'
		and ods.enabled_flag = 'Y'
		and ods.is_deleted_flg <> 'Y'
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_lookups'
				and batch_name = 'ap')
				 )
);

commit;
-- Soft Delete Records
update bec_dwh.dim_lookups set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
    SELECT 1
    FROM bec_ods.fnd_lookup_values ods
    WHERE NVL(dim_lookups.lookup_type, 'NA') = NVL(ods.lookup_type, 'NA')
    AND NVL(dim_lookups.lookup_code, 'NA') = NVL(ods.lookup_code, 'NA')
    AND NVL(dim_lookups.language, 'NA') = NVL(ods.language, 'NA')
    AND NVL(dim_lookups.view_application_id, 0) = NVL(ods.view_application_id, 0)
    AND ods.language = 'US'
    AND ods.enabled_flag = 'Y'
    AND ods.is_deleted_flg <> 'Y'
);
commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_lookups'
	and batch_name = 'ap';

commit;