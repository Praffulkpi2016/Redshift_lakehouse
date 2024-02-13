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

drop table if exists bec_dwh.DIM_ASSET_BOOK_CONTROL;

create table bec_dwh.DIM_ASSET_BOOK_CONTROL
	diststyle all sortkey(LEDGER_ID,BOOK_TYPE_CODE)
as
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
	)
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_asset_book_control'
	and batch_name = 'fa';

commit;