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

drop table if exists bec_dwh.DIM_FA_LOCATION ;

create table bec_dwh.dim_fa_location 
	diststyle all sortkey(location_id)
as
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
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_location'
	and batch_name = 'fa';

commit;