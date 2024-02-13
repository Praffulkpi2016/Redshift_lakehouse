/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by poplicable law or agreed to in writing, software
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

delete from bec_dwh.dim_po_locations
where (nvl(LOCATION_ID,0)) in (
select nvl(ods.LOCATION_ID,0) as LOCATION_ID from bec_dwh.dim_po_locations dw, bec_ods.hr_locations_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.LOCATION_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_locations' and batch_name = 'po')
 )
);

commit;

-- Insert records

insert into bec_dwh.DIM_PO_LOCATIONS
(
	location_id,
	location_code,
	address_line_1,
	address_line_2,
    address_line_3,
	bill_to_site_flag,
	country,
    location_desc,
	postal_code,
	telephone_number_1,
    telephone_number_2,
	telephone_number_3,
	town_or_city,
	creation_date,
    derived_locale,
	CREATED_BY,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
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
    || '-' || nvl(LOCATION_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.hr_locations_all
Where	1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_po_locations' and batch_name = 'po')
 )
);

-- Soft delete

update bec_dwh.dim_po_locations set is_deleted_flg = 'Y'
where (nvl(LOCATION_ID,0) ) not in (
select nvl(ods.LOCATION_ID,0)  as LOCATION_ID from bec_dwh.dim_po_locations dw, bec_ods.hr_locations_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.LOCATION_ID,0) 
AND ods.is_deleted_flg <>'Y');

commit;

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_locations' and batch_name = 'po';

COMMIT; 