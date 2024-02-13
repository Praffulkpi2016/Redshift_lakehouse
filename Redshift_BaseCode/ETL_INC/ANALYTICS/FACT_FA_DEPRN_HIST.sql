/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;
	-- Delete Records
	
	delete
	from
	bec_dwh.FACT_FA_DEPRN_HIST
	where
	(
		nvl(asset_id, 0),
		nvl(distribution_id, 0),
		nvl(book_type_code, 'NA'),
		nvl(period_name, 'NA')
	)
	in
	(
		select
		nvl(ods.asset_id, 0) as asset_id,
		nvl(ods.distribution_id, 0) as distribution_id,
		nvl(ods.book_type_code, 'NA') as book_type_code,
		nvl(ods.period_name, 'NA') as period_name
		from
		bec_dwh.FACT_FA_DEPRN_HIST dw,
		(
			select
			asset_id,
			book_type_code,
			distribution_id,
			period_name
			from
			((
				with cte_asgn_loc as (
					select assigned_to,location_id,asset_id,is_deleted_flg from bec_ods.fa_distribution_history fdh 
					where distribution_id = (select max(distribution_id) from bec_ods.fa_distribution_history
						where asset_id = fdh.asset_id
					)
				)
				select
				dh.asset_id asset_id,
				cb.book_type_code,
				dd_bonus.distribution_id,
				dp.period_name
				from   bec_ods.fa_deprn_detail   dd_bonus
				inner join   bec_ods.fa_category_books   cb
				on dd_bonus.book_type_code = cb.book_type_code
				inner join   bec_ods.fa_distribution_history   dh
				on dd_bonus.distribution_id = dh.distribution_id
				inner join   bec_ods.fa_books  books
				on cb.book_type_code = books.book_type_code
				and dh.asset_id = books.asset_id
				inner join   bec_ods.fa_asset_history   ah
				on cb.category_id = ah.category_id
				and dh.asset_id = ah.asset_id
				inner join (
					select
					bc.distribution_source_book dist_book,
					nvl(dp.period_close_date, sysdate) ucd,
					dp.period_counter upc,
					MIN(dp_fy.period_open_date) tod,
					MIN(dp_fy.period_counter) tpc,
					dp.period_name,
					bc.book_type_code,
					dp.is_deleted_flg,
					dp_fy.is_deleted_flg is_deleted_flg1,
					bc.is_deleted_flg is_deleted_flg2
					from
					bec_ods.fa_deprn_periods   dp,
					bec_ods.fa_deprn_periods   dp_fy,
					bec_ods.fa_book_controls   bc
					where
					dp.book_type_code = bc.book_type_code
					and dp_fy.book_type_code = bc.book_type_code
					and dp_fy.fiscal_year = dp.fiscal_year
					group by
					bc.distribution_source_book,
					dp.period_close_date,
					dp.period_counter,
					dp.period_name,
				bc.book_type_code,
				dp.is_deleted_flg,
					dp_fy.is_deleted_flg,
					bc.is_deleted_flg) dp
				on dh.book_type_code = dp.dist_book
				and dh.date_effective <= dp.ucd
				and nvl(dh.date_ineffective, sysdate) > dp.tod
				and books.book_type_code = dp.book_type_code
				and nvl(books.period_counter_fully_retired, dp.upc) >= dp.tpc
				and dd_bonus.period_counter = (
					select
					MAX(dd_sub.period_counter)
					from
					bec_ods.fa_deprn_detail  dd_sub
					where
					dd_sub.book_type_code = cb.book_type_code
					and dd_sub.asset_id = dh.asset_id
					and dd_sub.distribution_id = dh.distribution_id
					and dd_sub.period_counter <= dp.upc
				)
				inner join   bec_ods.fa_transaction_headers   th_rt
				on cb.book_type_code = th_rt.book_type_code
				and books.transaction_header_id_in = th_rt.transaction_header_id
				left outer join   bec_ods.fa_transaction_headers   th
				on dp.dist_book = th.book_type_code
				and dh.transaction_header_id_out = th.transaction_header_id
				and th.date_effective between dp.tod and dp.ucd	
				left outer join  bec_ods.fa_deprn_summary   ds
				on dp.upc = ds.period_counter
				and books.book_type_code = ds.book_type_code
				and books.asset_id = ds.asset_id
				left outer join (select sum(attribute3) invoice_price,
					sum(attribute4) frieght_insurance,
					sum(attribute6) installation,
					sum(attribute7) other,
					asset_id,
					is_deleted_flg  from bec_ods.fa_asset_invoices 
				group by asset_id,is_deleted_flg) inv
				on inv.asset_id = dh.asset_id
				left outer join cte_asgn_loc
				on cte_asgn_loc.asset_id = dh.asset_id
				where 1=1
				and ah.asset_type = 'CAPITALIZED'
				and ah.date_effective < nvl(th.date_effective, dp.ucd)
				and nvl(ah.date_ineffective, sysdate) >= nvl(th.date_effective, dp.ucd) 	
				and books.date_effective <= nvl(th.date_effective, dp.ucd)
				and nvl(books.date_ineffective, sysdate + 1) > nvl(th.date_effective, dp.ucd)
				and ( dh.kca_seq_date >= (
					select
					(executebegints-prune_days)
					from
					bec_etl_ctrl.batch_dw_info
					where
					dw_table_name = 'fact_fa_deprn_hist'
				and batch_name = 'fa') 
				or dh.is_deleted_flg = 'Y'
				or cte_asgn_loc.is_deleted_flg = 'Y'
				or dd_bonus.is_deleted_flg = 'Y'
				or cb.is_deleted_flg = 'Y'
				or books.is_deleted_flg = 'Y'
				or ah.is_deleted_flg = 'Y'
				or dp.is_deleted_flg = 'Y'
				or dp.is_deleted_flg1 = 'Y'
				or dp.is_deleted_flg2 = 'Y'
				or th_rt.is_deleted_flg = 'Y'
				or th.is_deleted_flg = 'Y'
				or ds.is_deleted_flg = 'Y'
				or inv.is_deleted_flg = 'Y')
				)
				union all
				(with cte_asgn_loc as (
					select assigned_to,location_id,asset_id,is_deleted_flg  from bec_ods.fa_distribution_history fdh 
					where distribution_id = (select max(distribution_id) from bec_ods.fa_distribution_history
					)
				)
				select
				dh.asset_id asset_id,
				cb.book_type_code,
				dd.distribution_id,
				dp.period_name
				from  bec_ods.fa_deprn_detail   dd
				inner join  bec_ods.fa_category_books   cb
				on dd.book_type_code = cb.book_type_code
				inner join   bec_ods.fa_distribution_history   dh
				on dd.distribution_id = dh.distribution_id
				inner join   bec_ods.fa_books   books
				on cb.book_type_code = books.book_type_code
				and dh.asset_id = books.asset_id
				inner join   bec_ods.fa_asset_history   ah
				on cb.category_id = ah.category_id
				and dh.asset_id = ah.asset_id
				inner join (
					select
					bc.distribution_source_book dist_book,
					nvl(dp.period_close_date, sysdate) ucd,
					dp.period_counter upc,
					MIN(dp_fy.period_open_date) tod,
					MIN(dp_fy.period_counter) tpc,
					dp.period_name,
					bc.book_type_code,
					dp.is_deleted_flg,
					dp_fy.is_deleted_flg is_deleted_flg1,
					bc.is_deleted_flg is_deleted_flg2
					from
					bec_ods.fa_deprn_periods   dp,
					bec_ods.fa_deprn_periods   dp_fy,
					bec_ods.fa_book_controls   bc
					where
					dp.book_type_code = bc.book_type_code
					and dp_fy.book_type_code = bc.book_type_code
					and dp_fy.fiscal_year = dp.fiscal_year
					group by
					bc.distribution_source_book,
					dp.period_close_date,
					dp.period_counter,
					dp.period_name,
				bc.book_type_code,
				dp.is_deleted_flg,
					dp_fy.is_deleted_flg,
					bc.is_deleted_flg) dp
				on dh.book_type_code = dp.dist_book
				and dh.date_effective <= dp.ucd
				and nvl(dh.date_ineffective, sysdate) > dp.tod
				and books.book_type_code = dp.book_type_code
				and nvl(books.period_counter_fully_retired, dp.upc) >= dp.tpc
				and dd.period_counter = (
					select
					MAX(dd_sub.period_counter)
					from
					bec_ods.fa_deprn_detail   dd_sub
					where
					dd_sub.book_type_code = cb.book_type_code
					and dd_sub.asset_id = dh.asset_id
					and dd_sub.distribution_id = dh.distribution_id
					and dd_sub.period_counter <= dp.upc
				)
				inner join   bec_ods.fa_transaction_headers   th_rt
				on cb.book_type_code = th_rt.book_type_code
				and books.transaction_header_id_in = th_rt.transaction_header_id
				left outer join  bec_ods.fa_transaction_headers   th
				on dp.dist_book = th.book_type_code
				and dh.transaction_header_id_out = th.transaction_header_id
				and th.date_effective between dp.tod and dp.ucd	
				left outer join  bec_ods.fa_deprn_summary   ds
				on dp.upc = ds.period_counter
				and books.book_type_code = ds.book_type_code
				and books.asset_id = ds.asset_id
				left outer join (select sum(attribute3) invoice_price,
					sum(attribute4) frieght_insurance,
					sum(attribute6) installation,
					sum(attribute7) other
					,asset_id
					,is_deleted_flg from bec_ods.fa_asset_invoices 
				group by asset_id,is_deleted_flg) inv
				on inv.asset_id = dh.asset_id
				left outer join cte_asgn_loc
				on cte_asgn_loc.asset_id = dh.asset_id
				where 1=1
				and ah.asset_type = 'CAPITALIZED'
				and ah.date_effective < nvl(th.date_effective, dp.ucd)
				and nvl(ah.date_ineffective, sysdate) >= nvl(th.date_effective, dp.ucd) 	
				and books.date_effective <= nvl(th.date_effective, dp.ucd)
				and nvl(books.date_ineffective, sysdate + 1) > nvl(th.date_effective, dp.ucd)
				and books.bonus_rule is not null
				and ( dh.kca_seq_date >= (
					select
					(executebegints-prune_days)
					from
					bec_etl_ctrl.batch_dw_info
					where
					dw_table_name = 'fact_fa_deprn_hist'
				and batch_name = 'fa') 
				or dh.is_deleted_flg = 'Y'
				or cte_asgn_loc.is_deleted_flg = 'Y'
				or dd.is_deleted_flg = 'Y'
				or cb.is_deleted_flg = 'Y'
				or books.is_deleted_flg = 'Y'
				or ah.is_deleted_flg = 'Y'
				or dp.is_deleted_flg = 'Y'
				or dp.is_deleted_flg1 = 'Y'
				or dp.is_deleted_flg2 = 'Y'
				or th_rt.is_deleted_flg = 'Y'
				or th.is_deleted_flg = 'Y'
				or ds.is_deleted_flg = 'Y'
				or inv.is_deleted_flg = 'Y'				)
				)
			)
		)ods
		where
		1 = 1
		and dw.dw_load_id =
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
		where
		source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0)|| '-' || nvl(ods.distribution_id, 0)
		|| '-' || nvl(ods.book_type_code, 'NA')|| '-' || nvl(ods.period_name, 'NA')
	);
	-- Insert Records
	
	insert
	into
	bec_dwh.FACT_FA_DEPRN_HIST
	(asset_id,
		book_type_code,
		dh_ccid,
		deprn_reserve_acct,
		rate,
		capacity,
		cost,
		deprn_reserve,
		ytd_deprn,
		deprn_amount,
		"percent",
		transaction_type,
		period_counter,
		date_effective,
		bonus_deprn_expense_acct,
		duties_octroi,
		assigned_to,
		location_id,
		deprn_rate,
		category_id,
		gl_account,
		accu_deprn,
		distribution_id,
		addition_cost_to_clear,
		deprn_adjustment_amount,
		deprn_run_date,
		dh_distribution_id,
		deprn_source_code,
		event_id,
		period_name,
		system_deprn_amount,
		dps_deprn_amount,
		invoice_price,
		frieght_insurance,
		installation,
		other,
		ASSET_ID_KEY,
		LOCATION_ID_KEY,
		DISTRIBUTION_ID_KEY,
		category_id_KEY,
		event_id_KEY,
		is_deleted_flg,
		SOURCE_APP_ID,
		DW_LOAD_ID,
		DW_INSERT_DATE,
		DW_UPDATE_DATE
	)
	(select
		asset_id,
		book_type_code,
		dh_ccid,
		deprn_reserve_acct,
		rate,
		capacity,
		cost,
		deprn_reserve,
		ytd_deprn,
		deprn_amount,
		"percent",
		transaction_type,
		period_counter,
		date_effective,
		bonus_deprn_expense_acct,
		duties_octroi,
		assigned_to,
		location_id,
		deprn_rate,
		category_id,
		gl_account,
		accu_deprn,
		distribution_id,
		addition_cost_to_clear,
		deprn_adjustment_amount,
		deprn_run_date,
		dh_distribution_id,
		deprn_source_code,
		event_id,
		period_name,
		system_deprn_amount,
		dps_deprn_amount,
		invoice_price,
		frieght_insurance,
		installation,
		other,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')|| '-' || ASSET_ID as ASSET_ID_KEY,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')|| '-' || LOCATION_ID as LOCATION_ID_KEY,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')|| '-' || DISTRIBUTION_ID as DISTRIBUTION_ID_KEY,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')|| '-' || category_id as category_id_KEY,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')|| '-' || event_id as event_id_KEY,
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
		source_system = 'EBS')|| '-' || nvl(asset_id, 0)|| '-' || nvl(distribution_id, 0)
		|| '-' || nvl(book_type_code, 'NA')|| '-' || nvl(period_name, 'NA') as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		from
		(
			select
			asset_id,
			book_type_code,
			dh_ccid,
			deprn_reserve_acct,
			rate,
			capacity,
			cost,
			deprn_reserve,
			ytd_deprn,
			deprn_amount,
			"percent",
			decode(transaction_type, 'P', 'Partially Retired', 'F', 'Fully Retired',
				'T', 'Transferred Out', 'N', 'Non Depreciate', 'R',
			'Reclass', 'B', 'Bonus', 'In Use') transaction_type,
			period_counter,
			date_effective,
			bonus_deprn_expense_acct,
			decode(transaction_type, 'B', null, cost) duties_octroi,
			assigned_to,
			location_id,
			deprn_rate,
			category_id,
			gl_account,
			accu_deprn,
			distribution_id,
			addition_cost_to_clear,
			deprn_adjustment_amount,
			deprn_run_date,
			dh_distribution_id,
			deprn_source_code,
			event_id,
			period_name,
			system_deprn_amount,
			dps_deprn_amount,
			invoice_price,
			frieght_insurance,
			installation,
			other
			from
			((with cte_asgn_loc as (
				select assigned_to,location_id,asset_id from bec_ods.fa_distribution_history fdh 	
				where distribution_id = (select max(distribution_id) from bec_ods.fa_distribution_history	
					where asset_id = fdh.asset_id	
				)
			)
			select
			dh.asset_id asset_id,
			cb.book_type_code,
			dh.code_combination_id dh_ccid,
			cb.deprn_reserve_acct deprn_reserve_acct,
			books.adjusted_rate rate,
			books.production_capacity capacity,
			dd_bonus.cost cost,
			decode(dd_bonus.period_counter, dp.upc, dd_bonus.deprn_amount - dd_bonus.bonus_deprn_amount, 0) deprn_amount,
			decode(sign(dp.tpc - dd_bonus.period_counter),
				1,
				0,
			dd_bonus.ytd_deprn - dd_bonus.bonus_ytd_deprn) ytd_deprn,
			dd_bonus.deprn_reserve - dd_bonus.bonus_deprn_reserve deprn_reserve,
			decode(th.transaction_type_code, null, dh.units_assigned / ah.units * 100) "percent",
			decode(th.transaction_type_code,
				null,
				decode(th_rt.transaction_type_code,
					'FULL RETIREMENT',
					'F',
				decode(books.depreciate_flag, 'NO', 'N')),
				'TRANSFER',
				'T',
				'TRANSFER OUT',
				'P',
				'RECLASS',
			'R') transaction_type,
			dp.upc period_counter,
			nvl(th.date_effective, dp.ucd) date_effective,
			'' bonus_deprn_expense_acct,
			cte_asgn_loc.assigned_to,
			cte_asgn_loc.location_id,
			books.basic_rate * 100 deprn_rate,
			ah.category_id,
			cb.asset_cost_acct gl_account,
			cb.deprn_reserve_acct accu_deprn,
			dd_bonus.distribution_id,
			dd_bonus.addition_cost_to_clear,
			dd_bonus.deprn_adjustment_amount,
			dd_bonus.deprn_run_date,
			dh.distribution_id dh_distribution_id,
			dd_bonus.deprn_source_code,
			dd_bonus.event_id,
			dp.period_name,
			ds.system_deprn_amount,
			ds.deprn_amount dps_deprn_amount,
			inv.invoice_price,
			inv.frieght_insurance,
			inv.installation,
			inv.other
			from (select * from bec_ods.fa_deprn_detail where is_deleted_flg <> 'Y') dd_bonus
			inner join (select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y') cb
			on dd_bonus.book_type_code = cb.book_type_code
			inner join (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') dh
			on dd_bonus.distribution_id = dh.distribution_id
			inner join (select * from bec_ods.fa_books where is_deleted_flg <> 'Y') books
			on cb.book_type_code = books.book_type_code
			and dh.asset_id = books.asset_id
			inner join (select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y') ah
			on cb.category_id = ah.category_id
			and dh.asset_id = ah.asset_id
			inner join (
				select
				bc.distribution_source_book dist_book,
				nvl(dp.period_close_date, sysdate) ucd,
				dp.period_counter upc,
				MIN(dp_fy.period_open_date) tod,
				MIN(dp_fy.period_counter) tpc,
				dp.period_name,
				bc.book_type_code
				from
				(select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') dp,
				(select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') dp_fy,
				(select * from bec_ods.fa_book_controls where is_deleted_flg <> 'Y') bc
				where
				dp.book_type_code = bc.book_type_code
				and dp_fy.book_type_code = bc.book_type_code
				and dp_fy.fiscal_year = dp.fiscal_year
				group by
				bc.distribution_source_book,
				dp.period_close_date,
				dp.period_counter,
				dp.period_name,
			bc.book_type_code) dp
			on dh.book_type_code = dp.dist_book
			and dh.date_effective <= dp.ucd
			and nvl(dh.date_ineffective, sysdate) > dp.tod
			and books.book_type_code = dp.book_type_code
			and nvl(books.period_counter_fully_retired, dp.upc) >= dp.tpc
			and dd_bonus.period_counter = (
				select
				MAX(dd_sub.period_counter)
				from
				(select * from bec_ods.fa_deprn_detail where is_deleted_flg <> 'Y') dd_sub
				where
				dd_sub.book_type_code = cb.book_type_code
				and dd_sub.asset_id = dh.asset_id
				and dd_sub.distribution_id = dh.distribution_id
				and dd_sub.period_counter <= dp.upc
			)
			inner join (select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') th_rt
			on cb.book_type_code = th_rt.book_type_code
			and books.transaction_header_id_in = th_rt.transaction_header_id
			left outer join (select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') th
			on dp.dist_book = th.book_type_code
			and dh.transaction_header_id_out = th.transaction_header_id
			and th.date_effective between dp.tod and dp.ucd	
			left outer join (select * from bec_ods.fa_deprn_summary where is_deleted_flg <> 'Y') ds
			on dp.upc = ds.period_counter
			and books.book_type_code = ds.book_type_code
			and books.asset_id = ds.asset_id
			left outer join (select sum(attribute3) invoice_price,
				sum(attribute4) frieght_insurance,
				sum(attribute6) installation,
				sum(attribute7) other
				,asset_id from bec_ods.fa_asset_invoices 
			group by asset_id) inv
			on inv.asset_id = dh.asset_id
			left outer join cte_asgn_loc
			on cte_asgn_loc.asset_id = dh.asset_id
			where 1=1
			and ah.asset_type = 'CAPITALIZED'
			and ah.date_effective < nvl(th.date_effective, dp.ucd)
			and nvl(ah.date_ineffective, sysdate) >= nvl(th.date_effective, dp.ucd) 	
			and books.date_effective <= nvl(th.date_effective, dp.ucd)
			and nvl(books.date_ineffective, sysdate + 1) > nvl(th.date_effective, dp.ucd)
			and ( dh.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_deprn_hist'
			and batch_name = 'fa'))
			)
			union all
			(with cte_asgn_loc as (
				select assigned_to,location_id,asset_id from bec_ods.fa_distribution_history fdh 
				where distribution_id = (select max(distribution_id) from bec_ods.fa_distribution_history
				)
			)
			select
			dh.asset_id asset_id,
			cb.book_type_code,
			dh.code_combination_id dh_ccid,
			cb.bonus_deprn_reserve_acct deprn_reserve_acct,
			books.adjusted_rate rate,
			books.production_capacity capacity,
			0 cost,
			decode(dd.period_counter, dp.upc, dd.bonus_deprn_amount, 0) deprn_amount,
			decode(sign(dp.tpc - dd.period_counter),
				1,
				0,
			dd.bonus_ytd_deprn) ytd_deprn,
			dd.bonus_deprn_reserve deprn_reserve,
			0 "percent",
			'B' transaction_type,
			dp.upc period_counter,
			nvl(th.date_effective, dp.ucd) date_effective,
			cb.bonus_deprn_expense_acct bonus_deprn_expense_acct,
			cte_asgn_loc.assigned_to,
			cte_asgn_loc.location_id,
			books.basic_rate * 100 deprn_rate,
			ah.category_id,
			cb.asset_cost_acct gl_account,
			cb.deprn_reserve_acct accu_deprn,
			dd.distribution_id,
			dd.addition_cost_to_clear,
			dd.deprn_adjustment_amount,
			dd.deprn_run_date,
			dh.distribution_id dh_distribution_id,
			dd.deprn_source_code,
			dd.event_id,
			dp.period_name,
			ds.system_deprn_amount,
			ds.deprn_amount dps_deprn_amount,
			inv.invoice_price,
			inv.frieght_insurance,
			inv.installation,
			inv.other
			from (select * from bec_ods.fa_deprn_detail where is_deleted_flg <> 'Y') dd
			inner join (select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y') cb
			on dd.book_type_code = cb.book_type_code
			inner join (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') dh
			on dd.distribution_id = dh.distribution_id
			inner join (select * from bec_ods.fa_books where is_deleted_flg <> 'Y') books
			on cb.book_type_code = books.book_type_code
			and dh.asset_id = books.asset_id
			inner join (select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y') ah
			on cb.category_id = ah.category_id
			and dh.asset_id = ah.asset_id
			inner join (
				select
				bc.distribution_source_book dist_book,
				nvl(dp.period_close_date, sysdate) ucd,
				dp.period_counter upc,
				MIN(dp_fy.period_open_date) tod,
				MIN(dp_fy.period_counter) tpc,
				dp.period_name,
				bc.book_type_code
				from
				(select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') dp,
				(select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') dp_fy,
				(select * from bec_ods.fa_book_controls where is_deleted_flg <> 'Y') bc
				where
				dp.book_type_code = bc.book_type_code
				and dp_fy.book_type_code = bc.book_type_code
				and dp_fy.fiscal_year = dp.fiscal_year
				group by
				bc.distribution_source_book,
				dp.period_close_date,
				dp.period_counter,
				dp.period_name,
			bc.book_type_code) dp
			on dh.book_type_code = dp.dist_book
			and dh.date_effective <= dp.ucd
			and nvl(dh.date_ineffective, sysdate) > dp.tod
			and books.book_type_code = dp.book_type_code
			and nvl(books.period_counter_fully_retired, dp.upc) >= dp.tpc
			and dd.period_counter = (
				select
				MAX(dd_sub.period_counter)
				from
				(select * from bec_ods.fa_deprn_detail where is_deleted_flg <> 'Y') dd_sub
				where
				dd_sub.book_type_code = cb.book_type_code
				and dd_sub.asset_id = dh.asset_id
				and dd_sub.distribution_id = dh.distribution_id
				and dd_sub.period_counter <= dp.upc
			)
			inner join (select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') th_rt
			on cb.book_type_code = th_rt.book_type_code
			and books.transaction_header_id_in = th_rt.transaction_header_id
			left outer join (select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') th
			on dp.dist_book = th.book_type_code
			and dh.transaction_header_id_out = th.transaction_header_id
			and th.date_effective between dp.tod and dp.ucd	
			left outer join (select * from bec_ods.fa_deprn_summary where is_deleted_flg <> 'Y') ds
			on dp.upc = ds.period_counter
			and books.book_type_code = ds.book_type_code
			and books.asset_id = ds.asset_id
			left outer join (select sum(attribute3) invoice_price,
				sum(attribute4) frieght_insurance,
				sum(attribute6) installation,
				sum(attribute7) other
				,asset_id from bec_ods.fa_asset_invoices 
			group by asset_id) inv
			on inv.asset_id = dh.asset_id
			left outer join cte_asgn_loc
			on cte_asgn_loc.asset_id = dh.asset_id
			where 1=1
			and ah.asset_type = 'CAPITALIZED'
			and ah.date_effective < nvl(th.date_effective, dp.ucd)
			and nvl(ah.date_ineffective, sysdate) >= nvl(th.date_effective, dp.ucd) 	
			and books.date_effective <= nvl(th.date_effective, dp.ucd)
			and nvl(books.date_ineffective, sysdate + 1) > nvl(th.date_effective, dp.ucd)
			and books.bonus_rule is not null
			and ( dh.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_deprn_hist'
			and batch_name = 'fa'))
			)
			)
		));
		
		commit;
		
		end;
		
		update
		bec_etl_ctrl.batch_dw_info
		set
		load_type = 'I',
		last_refresh_date = getdate()
		where
		dw_table_name = 'fact_fa_deprn_hist'
		and batch_name = 'fa';
		
		commit;		