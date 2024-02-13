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
	bec_dwh.dim_fa_location
where
	nvl(location_id, 0)
 in (
	select
		nvl(ods.location_id, 0) as location_id
	from
		bec_dwh.dim_fa_location dw,
		(
		select
			fl.location_id
		from
			bec_ods.fa_locations fl
		where
			(fl.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_location'
				and batch_name = 'fa')	
				 )
	) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.location_id, 0));

commit;
-- Insert records

insert
	into
	bec_dwh.dim_fa_location
(
	location_id,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	enabled_flag,
	end_date_active,
	last_update_date,
	last_updated_by,
	x_custom,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		fl.location_id,
		fl.segment1,
		fl.segment2,
		fl.segment3,
		fl.segment4,
		fl.segment5,
		fl.segment6,
		fl.segment7,
		fl.enabled_flag,
		fl.end_date_active,
		fl.last_update_date,
		fl.last_updated_by ,
		'0' as x_custom,
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
			source_system = 'EBS')|| '-' || nvl(fl.location_id, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.fa_locations fl
	where
			(fl.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_location'
				and batch_name = 'fa')	
				 )

);
-- Soft delete

update
	bec_dwh.dim_fa_location
set
	is_deleted_flg = 'Y'
where
	 NVL(location_id, 0 ) not in (
	select
		nvl(ods.location_id, 0) as location_id
	from
		bec_dwh.dim_fa_location dw,
		(
		select
			fl.location_id
		from
			bec_ods.fa_locations fl
		where is_deleted_flg <> 'Y'	
	) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.location_id, 0));

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_location'
	and batch_name = 'fa';

commit;