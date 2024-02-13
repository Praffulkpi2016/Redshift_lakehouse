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

drop table if exists bec_dwh.DIM_AP_SUPPLIERS;

create table bec_dwh.DIM_AP_SUPPLIERS 
diststyle all 
sortkey(vendor_id)
as
(
select
	vendor_id,
	vendor_name,
	segment1 as vendor_number,
	vendor_name_alt,
	vendor_type_lookup_code,
	enabled_flag,
	employee_id,
	terms_id,
	invoice_currency_code,
	payment_currency_code,
	start_date_active,
	end_date_active,
	party_id,
	trim(attribute2) as vendor_category,
	created_by,
	last_updated_by,
	creation_date,
	last_update_date,
	num_1099,
	type_1099,
	RECEIPT_REQUIRED_FLAG,
	INSPECTION_REQUIRED_FLAG,
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
       || nvl(vendor_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.ap_suppliers
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ap_suppliers'
	and batch_name = 'ap';

commit;
