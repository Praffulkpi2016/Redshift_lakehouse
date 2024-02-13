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
	bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL
where
	(nvl(asset_id, 0),
	nvl(code_combination_id, 0)) in (
	select
		nvl(ods.asset_id, 0) as asset_id,
		nvl(ods.code_combination_id, 0) as code_combination_id
	from
		bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL dw,
		(
		select
			fact_asset_transaction.asset_id,
			fact_asset_transaction.code_combination_id
		from
			(
			select
				fa_adjustments.source_type_code source_type_code,
				fa_adjustments.adjustment_type adjustment_type,
				fa_distribution_history.code_combination_id code_combination_id,
				fa_transaction_headers.transaction_header_id transaction_header_id,
				fa_adjustments.asset_id,
				fa_adjustments.last_update_date,
				fa_adjustments.kca_seq_date
			from
				bec_ods.fa_distribution_history fa_distribution_history
			inner join bec_ods.fa_adjustments fa_adjustments on
				fa_adjustments.distribution_id = fa_distribution_history.distribution_id
			inner join bec_ods.fa_transaction_headers fa_transaction_headers
		on
				fa_transaction_headers.transaction_header_id = fa_adjustments.transaction_header_id
	) fact_asset_transaction
		inner join
	(
			select
				fdd.asset_id asset_id,
				fdd.deprn_run_date deprn_run_date
			from
				bec_ods.fa_deprn_detail fdd
			inner join bec_ods.fa_deprn_summary fds on
				fdd.asset_id = fds.asset_id
				and fdd.book_type_code = fds.book_type_code
				and fdd.period_counter = fds.period_counter 
	) fact_deprn_hist
on
			fact_asset_transaction.asset_id = fact_deprn_hist.asset_id
		inner join bec_ods.gl_code_combinations_kfv gl_code
on
			fact_asset_transaction.code_combination_id = gl_code.code_combination_id
			and fact_asset_transaction.source_type_code = 'ADDITION'
			and fact_asset_transaction.adjustment_type = 'COST'
		where
			(fact_asset_transaction.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset_additional_detail'
				and batch_name = 'fa')
				 )
		group by
			fact_asset_transaction.asset_id,
			fact_asset_transaction.code_combination_id
) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0) || '-' || nvl(ods.code_combination_id, 0)
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL
(
asset_id,
	code_combination_id,
	concatenated_segments,
	deprn_run_date,
	transaction_header_id,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		fact_asset_transaction.asset_id,
		fact_asset_transaction.code_combination_id,
		gl_code.segment1 || '-' || gl_code.segment2 || '-' || gl_code.segment3 || '-' || gl_code.segment4 ||
	gl_code.segment5 || '-' || gl_code.segment6 concatenated_segments,
		max(fact_deprn_hist.deprn_run_date) as deprn_run_date,
		max(fact_asset_transaction.transaction_header_id) as transaction_header_id,
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
    || '-' || nvl(fact_asset_transaction.asset_id, 0)
	|| '-' || nvl(fact_asset_transaction.code_combination_id, 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		(
		select
			fa_adjustments.source_type_code source_type_code,
			fa_adjustments.adjustment_type adjustment_type,
			fa_distribution_history.code_combination_id code_combination_id,
			fa_transaction_headers.transaction_header_id transaction_header_id,
			fa_adjustments.asset_id,
			fa_adjustments.last_update_date,
			fa_adjustments.kca_seq_date
		from
			bec_ods.fa_distribution_history fa_distribution_history
		inner join bec_ods.fa_adjustments fa_adjustments on
			fa_adjustments.distribution_id = fa_distribution_history.distribution_id
		inner join bec_ods.fa_transaction_headers fa_transaction_headers
		on
			fa_transaction_headers.transaction_header_id = fa_adjustments.transaction_header_id
	) fact_asset_transaction
	inner join
	(
		select
			fdd.asset_id asset_id,
			fdd.deprn_run_date deprn_run_date
		from
			bec_ods.fa_deprn_detail fdd
		inner join bec_ods.fa_deprn_summary fds on
			fdd.asset_id = fds.asset_id
			and fdd.book_type_code = fds.book_type_code
			and fdd.period_counter = fds.period_counter 
	) fact_deprn_hist
on
		fact_asset_transaction.asset_id = fact_deprn_hist.asset_id
	inner join bec_ods.gl_code_combinations_kfv gl_code
on
		fact_asset_transaction.code_combination_id = gl_code.code_combination_id
		and fact_asset_transaction.source_type_code = 'ADDITION'
		and fact_asset_transaction.adjustment_type = 'COST'
	where
		(fact_asset_transaction.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset_additional_detail'
				and batch_name = 'fa')
				 )
	group by
		fact_asset_transaction.asset_id,
		fact_asset_transaction.code_combination_id,
		gl_code.segment1 || '-' || gl_code.segment2 || '-' || gl_code.segment3 || '-' || gl_code.segment4 ||
	gl_code.segment5 || '-' || gl_code.segment6 		
	   );
-- Soft delete

update
	bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL
set
	is_deleted_flg = 'Y'
where
	(nvl(asset_id, 0),
	nvl(code_combination_id, 0)) not in (
	select
		nvl(ods.asset_id, 0) as asset_id,
		nvl(ods.code_combination_id, 0) as code_combination_id
	from
		bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL dw,
		(
		select
			fact_asset_transaction.asset_id,
			fact_asset_transaction.code_combination_id
		from
			(
			select
				fa_adjustments.source_type_code source_type_code,
				fa_adjustments.adjustment_type adjustment_type,
				fa_distribution_history.code_combination_id code_combination_id,
				fa_transaction_headers.transaction_header_id transaction_header_id,
				fa_adjustments.asset_id
			from
				(select * from bec_ods.fa_distribution_history 
				where is_deleted_flg <> 'Y') fa_distribution_history
			inner join (select * from bec_ods.fa_adjustments 
				where is_deleted_flg <> 'Y') fa_adjustments on
				fa_adjustments.distribution_id = fa_distribution_history.distribution_id
			inner join (select * from bec_ods.fa_transaction_headers 
				where is_deleted_flg <> 'Y') fa_transaction_headers
		on
				fa_transaction_headers.transaction_header_id = fa_adjustments.transaction_header_id
	) fact_asset_transaction
		inner join
	(
			select
				fdd.asset_id asset_id,
				fdd.deprn_run_date deprn_run_date
			from
				(select * from bec_ods.fa_deprn_detail 
				where is_deleted_flg <> 'Y') fdd
			inner join (select * from bec_ods.fa_deprn_summary 
				where is_deleted_flg <> 'Y') fds on
				fdd.asset_id = fds.asset_id
				and fdd.book_type_code = fds.book_type_code
				and fdd.period_counter = fds.period_counter 
	) fact_deprn_hist
on
			fact_asset_transaction.asset_id = fact_deprn_hist.asset_id
		inner join (select * from bec_ods.gl_code_combinations_kfv 
				where is_deleted_flg <> 'Y') gl_code
on
			fact_asset_transaction.code_combination_id = gl_code.code_combination_id
			and fact_asset_transaction.source_type_code = 'ADDITION'
			and fact_asset_transaction.adjustment_type = 'COST'
		group by
			fact_asset_transaction.asset_id,
			fact_asset_transaction.code_combination_id
) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0) || '-' || nvl(ods.code_combination_id, 0)
);

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_asset_additional_detail'
	and batch_name = 'fa';

commit;