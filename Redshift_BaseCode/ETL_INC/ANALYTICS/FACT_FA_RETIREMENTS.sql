/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents incremental load approach for Facts.
	# File Version: KPI v1.0
*/

begin;
	-- Delete Records
	
	delete
	from
	bec_dwh.FACT_FA_RETIREMENTS
	where
	(nvl(retirement_id, 0),
	nvl(adjustment_line_id, 0)) in (
		select
		nvl(ods.retirement_id, 0) as retirement_id,
		nvl(ods.adjustment_line_id, 0) as adjustment_line_id
		from
		bec_dwh.FACT_FA_RETIREMENTS dw,
		(
			Select 
			retirement_id,
			adjustment_line_id 
			from 
			(
				SELECT 
				fr.retirement_id, 
				faj.adjustment_line_id
				FROM 
				bec_ods.fa_transaction_headers   fth, 
				bec_ods.fa_books   books, 
				bec_ods.fa_retirements   fr, 
				bec_ods.fa_adjustments   faj, 
				bec_ods.fa_distribution_history   fdh, 
				bec_ods.fa_asset_history   ah, 
				bec_ods.fa_category_books   cb 
				WHERE 
				fth.transaction_key = 'R' 
				AND fth.book_type_code = fr.book_type_code 
				AND fth.asset_id = fr.asset_id 
				AND fr.asset_id = books.asset_id 
				AND decode (
					fth.transaction_type_code, 'REINSTATEMENT', 
					fr.transaction_header_id_out, fr.transaction_header_id_in
				) = fth.transaction_header_id 
				AND faj.asset_id = fr.asset_id 
				AND faj.book_type_code = fr.book_type_code 
				AND faj.transaction_header_id = fth.transaction_header_id 
				AND ah.asset_id = fth.asset_id 
				AND ah.date_effective <= fth.date_effective 
				AND nvl(
					ah.date_ineffective, fth.date_effective + 1
				) > fth.date_effective 
				AND books.transaction_header_id_out = fth.transaction_header_id 
				AND books.book_type_code = fth.book_type_code 
				AND books.asset_id = fth.asset_id 
				AND cb.category_id = ah.category_id 
				AND cb.book_type_code = fr.book_type_code 
				AND fdh.distribution_id = faj.distribution_id 
				AND fth.asset_id = fdh.asset_id 
				and ( 
					fr.kca_seq_date > (
						select
						(executebegints-prune_days)
						from
						bec_etl_ctrl.batch_dw_info
						where
						dw_table_name = 'fact_fa_retirements'
					and batch_name = 'fa')
					or 
					faj.kca_seq_date > (
						select
						(executebegints-prune_days)
						from
						bec_etl_ctrl.batch_dw_info
						where
						dw_table_name = 'fact_fa_retirements'
					and batch_name = 'fa') 
					
					or fth.is_deleted_flg = 'Y'
					or books.is_deleted_flg = 'Y'
					or fr.is_deleted_flg = 'Y'
					or faj.is_deleted_flg = 'Y'
					or fdh.is_deleted_flg = 'Y'
					or ah.is_deleted_flg = 'Y'
					or cb.is_deleted_flg = 'Y'
				)
				UNION 
				SELECT 
				fr.retirement_id,
				NULL adjustment_line_id
				FROM 
				bec_ods.fa_transaction_headers  fth, 
				bec_ods.fa_books   books, 
				bec_ods.fa_retirements   fr, 
				(
					SELECT 
					DH.*, 
					TH2.DATE_EFFECTIVE TH2_DATE, 
					TH1.DATE_EFFECTIVE TH1_DATE ,
					
					DH.is_deleted_flg is_deleted_flg1,
					TH1.is_deleted_flg is_deleted_flg2,
					BC.is_deleted_flg is_deleted_flg3,
					TH2.is_deleted_flg is_deleted_flg4
					
					FROM 
					bec_ods.FA_TRANSACTION_HEADERS   TH1, 
					bec_ods.FA_DISTRIBUTION_HISTORY   DH, 
					bec_ods.FA_BOOK_CONTROLS   BC, 
					bec_ods.FA_TRANSACTION_HEADERS   TH2 
					WHERE 
					TH1.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE 
					AND TH1.TRANSACTION_TYPE_CODE IN (
						'FULL RETIREMENT', 'PARTIAL RETIREMENT'
					) 
					AND TH1.ASSET_ID = DH.ASSET_ID 
					AND BC.BOOK_TYPE_CODE = th1.BOOK_TYPE_CODE 
					AND bC.DISTRIBUTION_SOURCE_BOOK = DH.BOOK_TYPE_CODE 
					AND TH1.DATE_EFFECTIVE <= NVL(
						DH.DATE_INEFFECTIVE, TH1.DATE_EFFECTIVE
					) 
					AND TH1.ASSET_ID = TH2.ASSET_ID 
					AND TH2.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE 
					AND th2.TRANSACTION_TYPE_CODE = 'REINSTATEMENT'  
					and th2.date_effective >= dh.DATE_EFFECTIVE
				) fdh, 
				bec_ods.fa_asset_history  ah, 
				bec_ods.fa_category_books   cb 
				WHERE 
				fth.transaction_key = 'R' 
				AND fth.book_type_code = fr.book_type_code 
				AND fth.asset_id = fr.asset_id 
				AND fr.asset_id = books.asset_id 
				AND fr.transaction_header_id_out = fth.transaction_header_id 
				/*AND  faj.asset_id    = fr.asset_id    
					AND  faj.book_type_code  = fr.book_type_code  
				AND  faj.transaction_header_id  = fth.transaction_header_id*/
				AND ah.asset_id = fth.asset_id 
				AND ah.date_effective <= fth.date_effective 
				AND nvl(
					ah.date_ineffective, fth.date_effective + 1
				) > fth.date_effective 
				AND books.transaction_header_id_out = fth.transaction_header_id 
				AND books.book_type_code = fth.book_type_code 
				AND books.asset_id = fth.asset_id 
				AND cb.category_id = ah.category_id 
				AND cb.book_type_code = fr.book_type_code --AND  fdh.distribution_id  = faj.distribution_id
				AND fth.asset_id = fdh.asset_id 
				AND FDH.BOOK_TYPE_CODE = fth.BOOK_TYPE_CODE 
				AND fdh.TH2_DATE >= fth.date_effective 
				AND fdh.TH1_DATE <= fth.date_effective 
				AND fth.TRANSACTION_TYPE_CODE = 'REINSTATEMENT' 
				AND fr.COST_RETIRED = 0 
				and fr.cost_of_removal = 0 
				and fr.proceeds_of_sale = 0
				and	( 
					fr.kca_seq_date > (
						select
						(executebegints-prune_days)
						from
						bec_etl_ctrl.batch_dw_info
						where
						dw_table_name = 'fact_fa_retirements'
					and batch_name = 'fa') 
					or fr.is_deleted_flg = 'Y'
					or fdh.is_deleted_flg1 = 'Y'
					or fdh.is_deleted_flg2 = 'Y'
					or fdh.is_deleted_flg3 = 'Y'
					or fdh.is_deleted_flg4 = 'Y'
					
					or fth.is_deleted_flg = 'Y'
					or books.is_deleted_flg = 'Y'
					or ah.is_deleted_flg = 'Y'
					or cb.is_deleted_flg = 'Y'
					
					
				)
			)) ods
			where
			dw.dw_load_id = 
			(
				select
				system_id
				from
				bec_etl_ctrl.etlsourceappid
				where
			source_system = 'EBS'
			) || '-' || nvl(ods.retirement_id, 0) 
			|| '-' || nvl(ods.adjustment_line_id, 0)
	);
	
	commit;
	-- Insert records
	
	insert
	into
	bec_dwh.FACT_FA_RETIREMENTS  
	(
		retirement_id, 
		book_type_code, 
		cost_retired, 
		retirement_status, 
		cost_of_removal, 
		nbv_retired, 
		gain_loss_amount, 
		proceeds_of_sale, 
		retirement_type_code, 
		created_by, 
		creation_date, 
		unrevalued_cost_retired, 
		retirement_prorate_convention, 
		reval_reserve_retired, 
		date_retired, 
		reserve_retired, 
		asset_id, 
		adjust_last_update_date, 
		last_updated_by, 
		book_type_code_faj, 
		adjustment_type, 
		source_type_code, 
		adjustment_amount, 
		transaction_header_id, 
		adjustment_line_id, 
		code_combination_id, 
		date_ineffective, 
		location_id, 
		distribution_id, 
		transaction_units, 
		last_update_date, 
		transaction_date_entered, 
		transaction_type_code, 
		date_effective, 
		asset_type, 
		account, 
		date_placed_in_service, 
		PROCEEDS_CLR, 
		cost, 
		nbv, 
		proceeds, 
		removal, 
		reval_rsv_ret, 
		code,
		retirement_id_key,
		asset_id_key,
		transaction_header_id_key,
		adjustment_line_id_key,
		code_combination_id_key,
		location_id_key,
		distribution_id_key,
		is_deleted_flg,
		source_app_id,
		dw_load_id,
		dw_insert_date,
		dw_update_date 
	)
	(
		Select 
		retirement_id, 
		book_type_code, 
		cost_retired, 
		retirement_status, 
		cost_of_removal, 
		nbv_retired, 
		gain_loss_amount, 
		proceeds_of_sale, 
		retirement_type_code, 
		created_by, 
		creation_date, 
		unrevalued_cost_retired, 
		retirement_prorate_convention, 
		reval_reserve_retired, 
		date_retired, 
		reserve_retired, 
		asset_id, 
		adjust_last_update_date, 
		last_updated_by, 
		book_type_code_faj, 
		adjustment_type, 
		source_type_code, 
		adjustment_amount, 
		transaction_header_id, 
		adjustment_line_id, 
		code_combination_id, 
		date_ineffective, 
		location_id, 
		distribution_id, 
		transaction_units, 
		last_update_date, 
		transaction_date_entered, 
		transaction_type_code, 
		date_effective, 
		asset_type, 
		account, 
		date_placed_in_service, 
		PROCEEDS_CLR, 
		cost, 
		nbv, 
		proceeds, 
		removal, 
		reval_rsv_ret, 
		code,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || retirement_id as retirement_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || asset_id as asset_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || transaction_header_id as transaction_header_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || adjustment_line_id as adjustment_line_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || code_combination_id as code_combination_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || location_id as location_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || distribution_id as distribution_id_key,
		-- audit columns
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
		|| '-' || nvl(retirement_id, 0)
		|| '-' || nvl(adjustment_line_id, 0)
		as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date 
		from 
		(
			SELECT 
			fr.retirement_id, 
			fr.book_type_code, 
			fr.cost_retired, 
			fr.status as retirement_status, 
			fr.cost_of_removal, 
			fr.nbv_retired, 
			fr.gain_loss_amount, 
			fr.proceeds_of_sale, 
			fr.retirement_type_code, 
			fr.created_by, 
			fr.creation_date, 
			fr.unrevalued_cost_retired, 
			fr.retirement_prorate_convention, 
			fr.reval_reserve_retired, 
			fr.date_retired, 
			fr.reserve_retired, 
			faj.asset_id, 
			faj.last_update_date adjust_last_update_date, 
			faj.last_updated_by, 
			faj.book_type_code book_type_code_faj, 
			faj.adjustment_type, 
			faj.source_type_code, 
			faj.adjustment_amount, 
			faj.transaction_header_id, 
			faj.adjustment_line_id, 
			fdh.code_combination_id code_combination_id, 
			nvl(
				fdh.date_ineffective, 
				to_date(
					'01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss'
				)
			) date_ineffective, 
			fdh.location_id, 
			fdh.distribution_id, 
			fdh.transaction_units, 
			fdh.last_update_date, 
			--  fdp.period_name adj_period_name,
			fth.transaction_date_entered, 
			fth.transaction_type_code, 
			fth.date_effective, 
			ah.asset_type, 
			decode (
				ah.asset_type, 'CIP', cb.cip_cost_acct, 
				cb.asset_cost_acct
			) account, 
			books.date_placed_in_service, 
			(
				select 
				distinct 'PROCEEDS' 
				from 
				(select * from bec_ods.fa_adjustments where is_deleted_flg <> 'Y') aj1 
				where 
				aj1.book_type_code = faj.book_type_code 
				and aj1.asset_id = faj.asset_id 
				and aj1.transaction_header_id = faj.transaction_header_id 
				and aj1.adjustment_type = 'PROCEEDS CLR'
			) PROCEEDS_CLR, 
			decode(
				faj.adjustment_type, 'COST', 1, 'CIP COST', 
			1, 0
			) * decode(
				faj.debit_credit_flag, 'DR', -1, 'CR', 
				1, 0
			) * faj.adjustment_amount cost, 
			decode(
				faj.adjustment_type, 'NBV RETIRED', 
			-1, 0
			) * decode(
				faj.debit_credit_flag, 'DR', -1, 'CR', 
				1, 0
			) * faj.adjustment_amount nbv, 
			decode(
				faj.adjustment_type, 'PROCEEDS CLR', 
			1, 'PROCEEDS', 1, 0
			) * decode(
				faj.debit_credit_flag, 'DR', 1, 'CR', 
				-1, 0
			) * faj.adjustment_amount proceeds, 
			decode(
				faj.adjustment_type, 'REMOVALCOST', 
			-1, 0
			) * decode(
				faj.debit_credit_flag, 'DR', -1, 'CR', 
				1, 0
			) * faj.adjustment_amount removal, 
			decode(
				faj.adjustment_type, 'REVAL RSV RET', 
			1, 0
			)* decode(
				faj.debit_credit_flag, 'DR',-1, 'CR', 
				1, 0
			)* faj.adjustment_amount reval_rsv_ret, 
			decode (
				fth.transaction_type_code, 
				'REINSTATEMENT', 
				'*', 
				'PARTIAL RETIREMENT', 
				'P', 
				(null):: VARCHAR
			) code 
			FROM 
			(select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') fth, 
			(select * from bec_ods.fa_books where is_deleted_flg <> 'Y') books, 
			(select * from bec_ods.fa_retirements where is_deleted_flg <> 'Y') fr, 
			(select * from bec_ods.fa_adjustments where is_deleted_flg <> 'Y') faj, 
			(select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') fdh, 
			(select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y') ah, 
			(select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y') cb 
			WHERE 
			fth.transaction_key = 'R' 
			AND fth.book_type_code = fr.book_type_code 
			AND fth.asset_id = fr.asset_id 
			AND fr.asset_id = books.asset_id 
			AND decode (
				fth.transaction_type_code, 'REINSTATEMENT', 
				fr.transaction_header_id_out, fr.transaction_header_id_in
			) = fth.transaction_header_id 
			AND faj.asset_id = fr.asset_id 
			AND faj.book_type_code = fr.book_type_code 
			AND faj.transaction_header_id = fth.transaction_header_id 
			AND ah.asset_id = fth.asset_id 
			AND ah.date_effective <= fth.date_effective 
			AND nvl(
				ah.date_ineffective, fth.date_effective + 1
			) > fth.date_effective 
			AND books.transaction_header_id_out = fth.transaction_header_id 
			AND books.book_type_code = fth.book_type_code 
			AND books.asset_id = fth.asset_id 
			AND cb.category_id = ah.category_id 
			AND cb.book_type_code = fr.book_type_code 
			AND fdh.distribution_id = faj.distribution_id 
			AND fth.asset_id = fdh.asset_id 
			and ( 
				fr.kca_seq_date > (
					select
					(executebegints-prune_days)
					from
					bec_etl_ctrl.batch_dw_info
					where
					dw_table_name = 'fact_fa_retirements'
				and batch_name = 'fa')
				or 
				faj.kca_seq_date > (
					select
					(executebegints-prune_days)
					from
					bec_etl_ctrl.batch_dw_info
					where
					dw_table_name = 'fact_fa_retirements'
				and batch_name = 'fa')
			)
			/*AND faj.adjustment_type NOT IN  (select  'PROCEEDS' from fa_adjustments aj1
				where aj1.book_type_code = faj.book_type_code
				and aj1.asset_id = faj.asset_id
				and aj1.transaction_header_id = faj.transaction_header_id
			and aj1.adjustment_type = 'PROCEEDS CLR') */
			UNION 
			SELECT 
			fr.retirement_id, 
			fr.book_type_code, 
			fr.cost_retired, 
			fr.status as retirement_status, 
			fr.cost_of_removal, 
			fr.nbv_retired, 
			fr.gain_loss_amount, 
			fr.proceeds_of_sale, 
			fr.retirement_type_code, 
			fr.created_by, 
			fr.creation_date, 
			fr.unrevalued_cost_retired, 
			fr.retirement_prorate_convention, 
			fr.reval_reserve_retired, 
			fr.date_retired, 
			fr.reserve_retired, 
			fr.asset_id, 
			NULL adjust_last_update_date, 
			NULL, 
			NULL, 
			NULL, 
			NULL, 
			NULL, 
			NULL, 
			NULL, 
			fdh.code_combination_id code_combination_id, 
			nvl(
				fdh.date_ineffective, 
				to_date(
					'01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss'
				)
			) date_ineffective, 
			fdh.location_id, 
			fdh.distribution_id, 
			fdh.transaction_units, 
			fdh.last_update_date, 
			--  fdp.period_name adj_period_name,
			fth.transaction_date_entered, 
			fth.transaction_type_code, 
			fth.date_effective, 
			ah.asset_type, 
			decode (
				ah.asset_type, 'CIP', cb.cip_cost_acct, 
				cb.asset_cost_acct
			) account, 
			books.date_placed_in_service, 
			NULL PROCEEDS_CLR, 
			---sum colums
			0 cost, 
			0 nbv, 
			nvl(fr.proceeds_of_sale, 0) proceeds, 
			nvl(fr.cost_of_removal, 0) removal, 
			0 reval_rsv_ret, 
			DECODE (
				fr.STATUS, 
				'DELETED', 
				'*', 
				(null):: Varchar
			) code 
			FROM 
			(select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') fth, 
			(select * from bec_ods.fa_books where is_deleted_flg <> 'Y') books, 
			(select * from bec_ods.fa_retirements where is_deleted_flg <> 'Y') fr, 
			(
				SELECT 
				DH.*, 
				TH2.DATE_EFFECTIVE TH2_DATE, 
				TH1.DATE_EFFECTIVE TH1_DATE 
				FROM 
				(select * from bec_ods.FA_TRANSACTION_HEADERS where is_deleted_flg <> 'Y') TH1, 
				(select * from bec_ods.FA_DISTRIBUTION_HISTORY where is_deleted_flg <> 'Y') DH, 
				(select * from bec_ods.FA_BOOK_CONTROLS where is_deleted_flg <> 'Y') BC, 
				(select * from bec_ods.FA_TRANSACTION_HEADERS where is_deleted_flg <> 'Y') TH2 
				WHERE 
				TH1.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE 
				AND TH1.TRANSACTION_TYPE_CODE IN (
					'FULL RETIREMENT', 'PARTIAL RETIREMENT'
				) 
				AND TH1.ASSET_ID = DH.ASSET_ID 
				AND BC.BOOK_TYPE_CODE = th1.BOOK_TYPE_CODE 
				AND bC.DISTRIBUTION_SOURCE_BOOK = DH.BOOK_TYPE_CODE 
				AND TH1.DATE_EFFECTIVE <= NVL(
					DH.DATE_INEFFECTIVE, TH1.DATE_EFFECTIVE
				) 
				AND TH1.ASSET_ID = TH2.ASSET_ID 
				AND TH2.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE 
				AND th2.TRANSACTION_TYPE_CODE = 'REINSTATEMENT'  
				and th2.date_effective >= dh.DATE_EFFECTIVE
			) fdh, 
			(select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y') ah, 
			(select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y') cb 
			WHERE 
			fth.transaction_key = 'R' 
			AND fth.book_type_code = fr.book_type_code 
			AND fth.asset_id = fr.asset_id 
			AND fr.asset_id = books.asset_id 
			AND fr.transaction_header_id_out = fth.transaction_header_id 
			/*AND  faj.asset_id    = fr.asset_id    
				AND  faj.book_type_code  = fr.book_type_code  
			AND  faj.transaction_header_id  = fth.transaction_header_id*/
			AND ah.asset_id = fth.asset_id 
			AND ah.date_effective <= fth.date_effective 
			AND nvl(
				ah.date_ineffective, fth.date_effective + 1
			) > fth.date_effective 
			AND books.transaction_header_id_out = fth.transaction_header_id 
			AND books.book_type_code = fth.book_type_code 
			AND books.asset_id = fth.asset_id 
			AND cb.category_id = ah.category_id 
			AND cb.book_type_code = fr.book_type_code --AND  fdh.distribution_id  = faj.distribution_id
			AND fth.asset_id = fdh.asset_id 
			AND FDH.BOOK_TYPE_CODE = fth.BOOK_TYPE_CODE 
			AND fdh.TH2_DATE >= fth.date_effective 
			AND fdh.TH1_DATE <= fth.date_effective 
			AND fth.TRANSACTION_TYPE_CODE = 'REINSTATEMENT' 
			AND fr.COST_RETIRED = 0 
			and fr.cost_of_removal = 0 
			and fr.proceeds_of_sale = 0
			and	( 
				fr.kca_seq_date > (
					select
					(executebegints-prune_days)
					from
					bec_etl_ctrl.batch_dw_info
					where
					dw_table_name = 'fact_fa_retirements'
				and batch_name = 'fa')
			)
		)
	);
	
	commit;
	
end;

update
bec_etl_ctrl.batch_dw_info
set
last_refresh_date = getdate()
where
dw_table_name = 'fact_fa_retirements'
and batch_name = 'fa';

commit;	