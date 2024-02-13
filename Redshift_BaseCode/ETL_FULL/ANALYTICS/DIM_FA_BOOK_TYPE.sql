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

drop table if exists bec_dwh.dim_fa_book_type;

create table bec_dwh.dim_fa_book_type 
	diststyle all sortkey(book_type_code,set_of_books_id)
as
(
select 
	fbc.book_type_code,
	fbc.book_type_name,
	fbc.set_of_books_id,
	fbc.last_deprn_run_date,
	fbc.book_class,
	fbc.last_update_date,
	fbc.last_updated_by,
	fbc.created_by,
	fbc.creation_date,
	fbc.date_ineffective,
	NVL(fbc.gl_posting_allowed_flag, 'NO') as gl_posting_allowed_flag,
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
		source_system = 'EBS')
	|| '-' || nvl(fbc.book_type_code, 'NA')
	|| '-' || nvl(fbc.set_of_books_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.fa_book_controls fbc
where
	1 = 1
	and fbc.date_ineffective is null
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_book_type'
	and batch_name = 'fa';

commit;