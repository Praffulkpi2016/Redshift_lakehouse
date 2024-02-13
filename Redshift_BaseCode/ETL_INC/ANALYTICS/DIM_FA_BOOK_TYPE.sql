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
	bec_dwh.dim_fa_book_type
where
	( NVL(book_type_code, 'NA'),
	NVL(set_of_books_id, 0)) in (
	select
		NVL(ods.book_type_code, 'NA') as book_type_code,
		NVL(ods.set_of_books_id, 0) as set_of_books_id
	from
		bec_dwh.dim_fa_book_type dw,
		(
		select
			fbc.book_type_code,
			fbc.set_of_books_id
		from
			bec_ods.fa_book_controls fbc
		where
			1 = 1
			and fbc.date_ineffective is null
			and (fbc.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_book_type'
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
			source_system = 'EBS')|| '-' || nvl(ods.book_type_code, 'NA')
	|| '-' || nvl(ods.set_of_books_id, 0));

commit;
-- Insert records

insert
	into
	bec_dwh.dim_fa_book_type
(
	book_type_code,
	book_type_name,
	set_of_books_id,
	last_deprn_run_date,
	book_class,
	last_update_date,
	last_updated_by,
	created_by,
	creation_date,
	date_ineffective,
	gl_posting_allowed_flag,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
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
		and (fbc.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_book_type'
				and batch_name = 'fa')
			 )

);
-- Soft delete

update
	bec_dwh.dim_fa_book_type
set
	is_deleted_flg = 'Y'
where
	( NVL(book_type_code, 'NA'),
	NVL(set_of_books_id, 0)) not in (
	select
		NVL(ods.book_type_code, 'NA') as book_type_code,
		NVL(ods.set_of_books_id, 0) as set_of_books_id
	from
		bec_dwh.dim_fa_book_type dw,
		(
		select
			fbc.book_type_code,
			fbc.set_of_books_id
		from
			bec_ods.fa_book_controls fbc
			where is_deleted_flg <> 'Y' 
			and 1 = 1
			and fbc.date_ineffective is null	
	) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.book_type_code, 'NA')
	|| '-' || nvl(ods.set_of_books_id, 0));

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_book_type'
	and batch_name = 'fa';

commit;