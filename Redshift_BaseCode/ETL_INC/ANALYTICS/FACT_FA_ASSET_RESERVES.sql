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
	bec_dwh.FACT_FA_ASSET_RESERVES
	where
	(
		nvl(TRANSACTION_HEADER_ID, 0),
		nvl(ADJUSTMENT_LINE_ID, 0),
		nvl(DISTRIBUTION_ID, 0)
	)
	in
	(
		select
		nvl(ods.TRANSACTION_HEADER_ID, 0) as TRANSACTION_HEADER_ID,
		nvl(ods.ADJUSTMENT_LINE_ID, 0) as ADJUSTMENT_LINE_ID,
		nvl(ods.DISTRIBUTION_ID, 0) as DISTRIBUTION_ID
		from
		bec_dwh.FACT_FA_ASSET_RESERVES dw,
		(
			SELECT
			DH.DISTRIBUTION_ID,
			AJ.ADJUSTMENT_LINE_ID,
			TH.TRANSACTION_HEADER_ID
			FROM
			BEC_ODS.FA_ADJUSTMENTS   AJ,
			BEC_ODS.FA_LOOKUPS_TL   RT,
			BEC_ODS.FA_DISTRIBUTION_HISTORY   DH,
			BEC_ODS.FA_ASSET_HISTORY   AH,
			BEC_ODS.FA_TRANSACTION_HEADERS   TH,
			BEC_ODS.XLA_AE_HEADERS   HEADERS,
			BEC_ODS.XLA_AE_LINES   LINES,
			BEC_ODS.XLA_DISTRIBUTION_LINKS   LINKS
			WHERE
			1 = 1
			AND AJ.ADJUSTMENT_TYPE = RT.LOOKUP_CODE
			AND RT.LOOKUP_TYPE = 'REPORT TYPE'
			AND RT.LOOKUP_CODE = 'RESERVE'
			AND AJ.ASSET_ID = DH.ASSET_ID
			AND AJ.DISTRIBUTION_ID = DH.DISTRIBUTION_ID
			AND AJ.ADJUSTMENT_TYPE IN ('RESERVE', DECODE('RESERVE', 'REVAL RESERVE', 'REVAL AMORT'))
			AND AH.ASSET_ID = DH.ASSET_ID
			AND ((AH.ASSET_TYPE <> 'EXPENSED' AND 'RESERVE' IN ('COST', 'CIP COST'))
			OR (AH.ASSET_TYPE IN ('CAPITALIZED', 'CIP') AND 'RESERVE' IN ('RESERVE', 'REVAL RESERVE')))
			AND AJ.TRANSACTION_HEADER_ID = TH.TRANSACTION_HEADER_ID 
			AND TH.TRANSACTION_HEADER_ID BETWEEN AH.TRANSACTION_HEADER_ID_IN AND NVL (AH.TRANSACTION_HEADER_ID_OUT - 1,TH.TRANSACTION_HEADER_ID)
			AND (DECODE (RT.LOOKUP_CODE,AJ.ADJUSTMENT_TYPE,1,0) * AJ.ADJUSTMENT_AMOUNT) <> 0
			AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_1 = AJ.TRANSACTION_HEADER_ID
			AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_2 = AJ.ADJUSTMENT_LINE_ID
			AND LINKS.APPLICATION_ID = 140
			AND LINKS.SOURCE_DISTRIBUTION_TYPE = 'TRX'
			AND HEADERS.APPLICATION_ID = 140
			AND HEADERS.AE_HEADER_ID = LINKS.AE_HEADER_ID
			AND LINES.AE_HEADER_ID = LINKS.AE_HEADER_ID
			AND LINES.AE_LINE_NUM = LINKS.AE_LINE_NUM
			AND LINES.APPLICATION_ID = 140
			AND ( AJ.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			dh.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			AH.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			TH.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			HEADERS.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			LINES.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa') 
			or AJ.is_deleted_flg = 'Y'
			or RT.is_deleted_flg = 'Y'
			or dh.is_deleted_flg = 'Y'
			or AH.is_deleted_flg = 'Y'
			or TH.is_deleted_flg = 'Y'
			or HEADERS.is_deleted_flg = 'Y'
			or LINES.is_deleted_flg = 'Y'
			or LINKS.is_deleted_flg = 'Y'
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
		source_system = 'EBS')|| '-' || nvl(ods.TRANSACTION_HEADER_ID, 0)|| '-' || nvl(ods.ADJUSTMENT_LINE_ID, 0)
	|| '-' || nvl(ods.DISTRIBUTION_ID, 0)
	);
	commit;
	-- Insert Records
	
	insert
	into
	bec_dwh.FACT_FA_ASSET_RESERVES
	(ASSET_ID,
		DISTRIBUTION_ID,
		ADJUSTMENT_LINE_ID,
		TRANSACTION_HEADER_ID,
		LEDGER_ID,
		DISTRIBUTION_CCID,
		ADJUSTMENT_CCID,
		CATEGORY_BOOKS_ACCOUNT,
		SOURCE_TYPE_CODE,
		BOOK_TYPE_CODE,
		REPORT_TYPE,
		PERIOD_COUNTER_CREATED,
		BEGING,
		ADDITION,
		DEPRECIATION,
		RECLASS,
		RETIREMENT,
		REVALUATION,
		TAX,
		TRANSFER,
		ENDING,
		is_deleted_flg,
		SOURCE_APP_ID,
		DW_LOAD_ID,
		DW_INSERT_DATE,
		DW_UPDATE_DATE
	)
	(SELECT
		ASSET_ID,
		DISTRIBUTION_ID,
		ADJUSTMENT_LINE_ID,
		TRANSACTION_HEADER_ID,
		LEDGER_ID,
		DISTRIBUTION_CCID,
		ADJUSTMENT_CCID,
		CATEGORY_BOOKS_ACCOUNT,
		SOURCE_TYPE_CODE,
		BOOK_TYPE_CODE,
		REPORT_TYPE,
		PERIOD_COUNTER_CREATED,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'BEGIN', NVL(AMOUNT, 0), NULL)) , 2) BEGING,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'ADDITION', NVL(AMOUNT, 0), NULL)) , 2) ADDITION,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'DEPRECIATION', NVL(AMOUNT, 0), NULL)) , 2) DEPRECIATION,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'RECLASS', NVL(AMOUNT, 0), NULL)), 2) RECLASS,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'RETIREMENT', -NVL(AMOUNT, 0), NULL)), 2) RETIREMENT,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'REVALUATION', NVL(AMOUNT, 0), NULL)), 2) REVALUATION,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'TAX', NVL(AMOUNT, 0), NULL)) , 2) TAX,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'TRANSFER', NVL(AMOUNT, 0), NULL)) , 2) TRANSFER,
		ROUND((DECODE (SOURCE_TYPE_CODE, 'END', NVL(AMOUNT, 0), NULL)), 2) ENDING,
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
		|| '-' || nvl(TRANSACTION_HEADER_ID, 0)
		|| '-' || nvl(ADJUSTMENT_LINE_ID, 0)
		|| '-' || nvl(DISTRIBUTION_ID, 0)
		as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		FROM
		(
			SELECT
			DH.ASSET_ID,
			DH.DISTRIBUTION_ID,
			AJ.ADJUSTMENT_LINE_ID,
			TH.TRANSACTION_HEADER_ID,
			HEADERS.LEDGER_ID,
			DH.CODE_COMBINATION_ID DISTRIBUTION_CCID,
			LINES.CODE_COMBINATION_ID ADJUSTMENT_CCID,
			NULL CATEGORY_BOOKS_ACCOUNT,
			AJ.SOURCE_TYPE_CODE,
			DH.BOOK_TYPE_CODE ,
			RT.LOOKUP_CODE REPORT_TYPE,
			AJ.PERIOD_COUNTER_CREATED,
			(DECODE (AJ.DEBIT_CREDIT_FLAG,'DR',1,-1) * AJ.ADJUSTMENT_AMOUNT) AMOUNT
			FROM
			(select * from BEC_ODS.FA_ADJUSTMENTS where is_deleted_flg <> 'Y') AJ,
			(select * from BEC_ODS.FA_LOOKUPS_TL where is_deleted_flg <> 'Y') RT,
			(select * from BEC_ODS.FA_DISTRIBUTION_HISTORY where is_deleted_flg <> 'Y') DH,
			(select * from BEC_ODS.FA_ASSET_HISTORY where is_deleted_flg <> 'Y') AH,
			(select * from BEC_ODS.FA_TRANSACTION_HEADERS where is_deleted_flg <> 'Y') TH,
			(select * from BEC_ODS.XLA_AE_HEADERS where is_deleted_flg <> 'Y') HEADERS,
			(select * from BEC_ODS.XLA_AE_LINES where is_deleted_flg <> 'Y') LINES,
			(select * from BEC_ODS.XLA_DISTRIBUTION_LINKS where is_deleted_flg <> 'Y') LINKS
			WHERE
			1 = 1
			AND AJ.ADJUSTMENT_TYPE = RT.LOOKUP_CODE
			AND RT.LOOKUP_TYPE = 'REPORT TYPE'
			AND RT.LOOKUP_CODE = 'RESERVE'
			AND AJ.ASSET_ID = DH.ASSET_ID
			AND AJ.DISTRIBUTION_ID = DH.DISTRIBUTION_ID
			AND AJ.ADJUSTMENT_TYPE IN ('RESERVE', DECODE('RESERVE', 'REVAL RESERVE', 'REVAL AMORT'))
			AND AH.ASSET_ID = DH.ASSET_ID
			AND ((AH.ASSET_TYPE <> 'EXPENSED' AND 'RESERVE' IN ('COST', 'CIP COST'))
			OR (AH.ASSET_TYPE IN ('CAPITALIZED', 'CIP') AND 'RESERVE' IN ('RESERVE', 'REVAL RESERVE')))
			AND AJ.TRANSACTION_HEADER_ID = TH.TRANSACTION_HEADER_ID 
			AND TH.TRANSACTION_HEADER_ID BETWEEN AH.TRANSACTION_HEADER_ID_IN AND NVL (AH.TRANSACTION_HEADER_ID_OUT - 1,TH.TRANSACTION_HEADER_ID)
			AND (DECODE (RT.LOOKUP_CODE,AJ.ADJUSTMENT_TYPE,1,0) * AJ.ADJUSTMENT_AMOUNT) <> 0
			AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_1 = AJ.TRANSACTION_HEADER_ID
			AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_2 = AJ.ADJUSTMENT_LINE_ID
			AND LINKS.APPLICATION_ID = 140
			AND LINKS.SOURCE_DISTRIBUTION_TYPE = 'TRX'
			AND HEADERS.APPLICATION_ID = 140
			AND HEADERS.AE_HEADER_ID = LINKS.AE_HEADER_ID
			AND LINES.AE_HEADER_ID = LINKS.AE_HEADER_ID
			AND LINES.AE_LINE_NUM = LINKS.AE_LINE_NUM
			AND LINES.APPLICATION_ID = 140
			AND ( AJ.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			dh.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			AH.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			TH.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			HEADERS.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			or
			LINES.kca_seq_date >= (
				select
				(executebegints-prune_days)
				from
				bec_etl_ctrl.batch_dw_info
				where
				dw_table_name = 'fact_fa_asset_reserves'
			and batch_name = 'fa')
			)
		)
	);
	
	commit;
	
end;

update
bec_etl_ctrl.batch_dw_info
set
load_type = 'I',
last_refresh_date = getdate()
where
dw_table_name = 'fact_fa_asset_reserves'
and batch_name = 'fa';

commit;