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
	bec_dwh.dim_ap_suppliers
where
	nvl(vendor_id, 0) in (
	select
		nvl(ods.vendor_id, 0)
	from
		bec_dwh.dim_ap_suppliers dw,
		bec_ods.ap_suppliers ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.vendor_id, 0)
			and (ods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ap_suppliers'
				and batch_name = 'ap'))
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_AP_SUPPLIERS
(
     vendor_id
	,vendor_name
	,vendor_number
	,vendor_name_alt
	,vendor_category
	,vendor_type_lookup_code
	,enabled_flag
	,employee_id
	,terms_id
	,invoice_currency_code
	,payment_currency_code
	,start_date_active
	,end_date_active
	,party_id
	,created_by
	,last_updated_by
	,creation_date
	,last_update_date
	,num_1099
	,type_1099
	,RECEIPT_REQUIRED_FLAG
	,INSPECTION_REQUIRED_FLAG
	,is_deleted_flg
	,source_app_id
	,dw_load_id
	,dw_insert_date
	,dw_update_date
)
(
	select
		vendor_id,
		vendor_name,
		segment1 as vendor_number,
		vendor_name_alt,
		trim(attribute2) as vendor_category,
		vendor_type_lookup_code,
		enabled_flag,
		employee_id,
		terms_id,
		invoice_currency_code,
		payment_currency_code,
		start_date_active,
		end_date_active,
		party_id,
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
	where
		1 = 1
		and (kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_dw_info
		where
			dw_table_name = 'dim_ap_suppliers'
			and batch_name = 'ap'))
 );
-- Soft delete

update
	bec_dwh.dim_ap_suppliers
set
	is_deleted_flg = 'Y'
where
	nvl(vendor_id, 0) not in (
	select
		nvl(ods.vendor_id, 0)
	from
		bec_dwh.dim_ap_suppliers dw,
		bec_ods.ap_suppliers ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.vendor_id, 0)
			and ods.is_deleted_flg <> 'Y');

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ap_suppliers'
	and batch_name = 'ap';

commit;