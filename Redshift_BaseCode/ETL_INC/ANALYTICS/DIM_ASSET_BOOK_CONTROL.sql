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
	bec_dwh.DIM_ASSET_BOOK_CONTROL
where
	(nvl(ledger_id, 0),
	nvl(book_type_code, 'NA')) in
(
	select
		nvl(ods.ledger_id, 0) as ledger_id,
		nvl(ods.book_type_code, 'NA') as book_type_code
	from
		bec_dwh.DIM_ASSET_BOOK_CONTROL dw,
		(
		select
			gl_ledgers.ledger_id,
			fa_book_controls.book_type_code
		from
			bec_ods.gl_ledgers gl_ledgers,
			bec_ods.fa_book_controls fa_book_controls
		where
			1 = 1
			and fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
			and (
			gl_ledgers.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 
			or
			fa_book_controls.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')				
				 
			)
	union all
		select
			gl_ledgers.ledger_id,
			fa_mc_book_controls.book_type_code
		from
			bec_ods.fa_mc_book_controls fa_mc_book_controls,
			bec_ods.fa_book_controls fa_book_controls,
			bec_ods.gl_ledgers gl_ledgers
		where
			1 = 1
			and gl_ledgers.ledger_id = fa_mc_book_controls.set_of_books_id
			and fa_book_controls.book_type_code = fa_mc_book_controls.book_type_code
			and (
			fa_book_controls.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 
			or	
			gl_ledgers.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 )
			
	) ods
	where
		dw.dw_load_id = 
						(
		select
								system_id
		from
								bec_etl_ctrl.etlsourceappid
		where
								source_system = 'EBS'
						)
						|| '-'
						|| nvl(ods.ledger_id, 0)
						|| '-'
						|| nvl(ods.book_type_code, 'NA')
);

commit;

-- Insert Records
insert
	into
	bec_dwh.DIM_ASSET_BOOK_CONTROL
(
	book_type_code,
	book_class,
	last_update_date,
	creation_date,
	name,
	currency_code,
	ledger_id,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		book_type_code,
		book_class,
		last_update_date,
		creation_date,
		name,
		currency_code,
		ledger_id,
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
    || nvl(ledger_id, 0)
	|| '-'
	|| nvl(book_type_code, 'NA') as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		(
		select
			fa_book_controls.book_type_code,
			fa_book_controls.book_class,
			fa_book_controls.last_update_date,
			fa_book_controls.creation_date,
			gl_ledgers.name,
			gl_ledgers.currency_code,
			gl_ledgers.ledger_id
		from
			bec_ods.gl_ledgers gl_ledgers,
			bec_ods.fa_book_controls fa_book_controls
		where
			1 = 1
			and fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
			and (
			fa_book_controls.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 
			or	
			gl_ledgers.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 )
	union all
		select
			fa_mc_book_controls.book_type_code,
			fa_book_controls.book_class,
			fa_mc_book_controls.last_update_date,
			fa_book_controls.creation_date,
			gl_ledgers.name,
			gl_ledgers.currency_code,
			gl_ledgers.ledger_id
		from
			bec_ods.fa_mc_book_controls fa_mc_book_controls,
			bec_ods.fa_book_controls fa_book_controls,
			bec_ods.gl_ledgers gl_ledgers
		where
			1 = 1
			and gl_ledgers.ledger_id = fa_mc_book_controls.set_of_books_id
			and fa_book_controls.book_type_code = fa_mc_book_controls.book_type_code
			and (
			fa_book_controls.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 
			or	
			gl_ledgers.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_book_control'
				and batch_name = 'fa')
				 )
	)
);

Commit;

-- Soft Delete
update
	bec_dwh.DIM_ASSET_BOOK_CONTROL
set
	is_deleted_flg = 'Y'
where
	(nvl(ledger_id, 0),
	nvl(book_type_code, 'NA')) not in 

	(select
		nvl(ods.ledger_id, 0) as ledger_id,
		nvl(ods.book_type_code, 'NA') as book_type_code
	from
		bec_dwh.DIM_ASSET_BOOK_CONTROL dw,
		(
		select
			gl_ledgers.ledger_id,
			fa_book_controls.book_type_code
		from
			(select * from bec_ods.gl_ledgers
			where is_deleted_flg<>'Y') gl_ledgers,
			(select * from bec_ods.fa_book_controls
			where is_deleted_flg<>'Y') fa_book_controls
		where
			1 = 1
			and fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
				union all
		select
			gl_ledgers.ledger_id,
			fa_mc_book_controls.book_type_code
		from
			(select * from bec_ods.fa_mc_book_controls
			where is_deleted_flg<>'Y') fa_mc_book_controls,
			(select * from bec_ods.fa_book_controls
			where is_deleted_flg<>'Y') fa_book_controls,
			(select * from bec_ods.gl_ledgers
			where is_deleted_flg<>'Y') gl_ledgers
		where
			1 = 1
			and gl_ledgers.ledger_id = fa_mc_book_controls.set_of_books_id
			and fa_book_controls.book_type_code = fa_mc_book_controls.book_type_code
			) ods
	where
		dw.dw_load_id = 
						(
		select
								system_id
		from
								bec_etl_ctrl.etlsourceappid
		where
								source_system = 'EBS'
						)
						|| '-'
						|| nvl(ods.ledger_id, 0)
						|| '-'
						|| nvl(ods.book_type_code, 'NA')
);

commit;

END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_asset_book_control'
	and batch_name = 'fa';

commit;