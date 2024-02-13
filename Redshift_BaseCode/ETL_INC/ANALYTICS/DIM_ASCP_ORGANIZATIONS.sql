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

truncate table bec_dwh.DIM_ASCP_ORGANIZATIONS;

insert into bec_dwh.DIM_ASCP_ORGANIZATIONS
(
select
    partner_id,
    sr_tp_id organization_id,
    sr_instance_id,
    organization_code,
    master_organization,
    partner_name,
    OPERATING_UNIT,
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
       || nvl(partner_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.msc_trading_partners
WHERE is_deleted_flg <> 'Y'
    and partner_type = 3
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ascp_organizations'
	and batch_name = 'ascp';

commit;