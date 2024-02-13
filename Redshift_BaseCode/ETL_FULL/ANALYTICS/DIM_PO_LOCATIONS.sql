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

drop table if exists bec_dwh.DIM_PO_LOCATIONS;

create table bec_dwh.DIM_PO_LOCATIONS diststyle all sortkey(location_id)
as 
(
select
	location_id,
	location_code,
	address_line_1,
	address_line_2,
    address_line_3,
	bill_to_site_flag,
	country,
    description location_desc,
	postal_code,
	telephone_number_1,
    telephone_number_2,
	telephone_number_3,
	town_or_city,
	creation_date,
    derived_locale,
	CREATED_BY,
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
       || nvl(location_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.hr_locations_all
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_po_locations'
	and batch_name = 'po';

commit;