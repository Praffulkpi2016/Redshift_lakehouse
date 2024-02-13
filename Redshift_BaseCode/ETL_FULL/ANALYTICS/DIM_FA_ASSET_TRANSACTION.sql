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

drop table if exists bec_dwh.dim_fa_asset_transaction;

create table bec_dwh.DIM_FA_ASSET_TRANSACTION 
	diststyle all sortkey(asset_invoice_id)
as
(
select 
	fai.asset_id, 
	fai.po_number, 
	fai.invoice_number, 
	fai.payables_batch_name, 
	fai.invoice_date, 
	fai.invoice_id, 
	fai.ap_distribution_line_number, 
	fai.asset_invoice_id, 
	fai.invoice_transaction_id_in, 
	fai.description, 
	fai.last_update_date, 
	fai.last_updated_by, 
	fai.created_by, 
	fai.creation_date, 
	fai.project_asset_line_id, 
	'0' as x_custom,
	fai.payables_cost,
	fai.ATTRIBUTE3,
	fai.ATTRIBUTE4,
	fai.ATTRIBUTE6,
	fai.ATTRIBUTE7,
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
		source_system = 'EBS')|| '-' || nvl(fai.asset_id, 0)|| '-' || nvl(fai.asset_invoice_id, 0) 
	|| '-' || nvl(fai.invoice_transaction_id_in, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.fa_asset_invoices fai
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_asset_transaction'
	and batch_name = 'fa';

commit;