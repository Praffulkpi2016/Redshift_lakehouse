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
	bec_dwh.DIM_FA_ASSET_TRANSACTION
where
	( nvl(asset_id, 0),
	NVL(asset_invoice_id, 0),
	NVL(invoice_transaction_id_in, 0)) in (
	select
		nvl(ods.asset_id, 0) as asset_id,
		NVL(ods.asset_invoice_id, 0) as asset_invoice_id,
		NVL(ods.invoice_transaction_id_in, 0) as invoice_transaction_id_in
	from
		bec_dwh.dim_fa_asset_transaction dw,
		(
		select
			fai.asset_id,
			fai.asset_invoice_id,
			fai.invoice_transaction_id_in
		from
			bec_ods.fa_asset_invoices fai
		where
			(fai.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset_transaction'
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
			source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0)
	|| '-' || nvl(ods.asset_invoice_id, 0)|| '-' || nvl(ods.invoice_transaction_id_in, 0));

commit;
-- Insert records

insert
	into
	bec_dwh.dim_fa_asset_transaction
(
	asset_id,
	po_number,
	invoice_number,
	payables_batch_name,
	invoice_date,
	invoice_id,
	ap_distribution_line_number,
	asset_invoice_id,
	invoice_transaction_id_in,
	description,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	project_asset_line_id,
	x_custom,
	payables_cost,
	attribute3,
	attribute4,
	attribute6,
	attribute7,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date

)
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
		fai.attribute3,
	    fai.attribute4,
	    fai.attribute6,
	    fai.attribute7,
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
	where
					(fai.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset_transaction'
				and batch_name = 'fa')
			 )	

);
-- Soft delete

update
	bec_dwh.dim_fa_asset_transaction
set
	is_deleted_flg = 'Y'
where
	( NVL(asset_id, 0 ),
	NVL(asset_invoice_id, 0),
	NVL(invoice_transaction_id_in, 0)) not in (
	select
		nvl(ods.asset_id, 0) as asset_id,
		NVL(ods.asset_invoice_id, 0) as asset_invoice_id,
		NVL(ods.invoice_transaction_id_in, 0) as invoice_transaction_id_in
	from
		bec_dwh.dim_fa_asset_transaction dw,
		(
		select
			fai.asset_id,
			fai.asset_invoice_id,
			fai.invoice_transaction_id_in
		from
			bec_ods.fa_asset_invoices fai
				where is_deleted_flg <> 'Y' 
	) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0)
	|| '-' || nvl(ods.asset_invoice_id, 0)|| '-' || nvl(ods.invoice_transaction_id_in, 0));

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_asset_transaction'
	and batch_name = 'fa';

commit;