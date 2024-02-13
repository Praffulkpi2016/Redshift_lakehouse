/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;
truncate  table bec_dwh.FACT_FA_COST_ROLLUP;
insert into bec_dwh.FACT_FA_COST_ROLLUP
select 
book_type_code
,account_type
,gl_account
,segment3_desc
,fiscal_year
,period_name
,period_num
,begin_balance
,end_balance
,additions
,adjustments
,retirements
,depreciations
,retirement_reserve
,transfer
,reclass
-- audit columns
,'N' as is_deleted_flg,
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
 || '-' || nvl(book_type_code,'NA')   || '-' || nvl(gl_account,'NA')|| '-' || nvl(period_name, 'NA')
as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
from
(
WITH accounts AS 
(
    SELECT DISTINCT
        fcb.asset_cost_acct         gl_account,
        fcb.asset_cost_account_ccid ccid,
        'Asset Account'             account_type,
        fcb.book_type_code,
		dga.segment1,
        dga.segment3,
        dga.segment3_desc,
        fbc.set_of_books_id
    FROM
        (select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y')fcb,
        bec_dwh.dim_gl_accounts   dga,
        (select * from bec_ods.fa_book_controls where is_deleted_flg <> 'Y')  fbc
    WHERE
            dga.code_combination_id = asset_cost_account_ccid
        AND fbc.book_type_code = fcb.book_type_code
    UNION
    SELECT DISTINCT
        fcb.deprn_reserve_acct   gl_account,
        fcb.reserve_account_ccid ccid,
        'Deprn Reserve Account'  account_type,
        fcb.book_type_code,
		dga.segment1,
        dga.segment3,
        dga.segment3_desc,
        fbc.set_of_books_id
    FROM
        (select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y') fcb,
        bec_dwh.dim_gl_accounts   dga,
        (select * from bec_ods.fa_book_controls where is_deleted_flg <> 'Y')  fbc
    WHERE
            dga.code_combination_id = reserve_account_ccid
        AND fbc.book_type_code = fcb.book_type_code
)
SELECT
    book_type_code,
    account_type,
    gl_account,
    segment3_desc,
    fiscal_year,
    period_name,
    period_num,
    SUM(begin_balance) begin_balance,
	SUM(end_balance) end_balance,
    SUM(additions)     additions,
    SUM(adjustments)   adjustments,
    SUM(retirement)    retirements,
    sum(depreciation)  depreciations,
    sum(retirement_reserve) retirement_reserve,
    sum(transfer) transfer,
    sum(reclass) reclass
FROM
    (
        SELECT
            ffaa.book_type_code,
            acc.account_type,
            ffaa.gl_account,
            acc.segment3_desc,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            0              begin_balance,
			0              end_balance,
            SUM(ffaa.cost) additions,
            0              adjustments,
            0              retirement,
            0  depreciation,
            0 retirement_reserve,
            0 transfer,
            0 reclass
        FROM
            bec_dwh.fact_fa_assets_addition ffaa,
            accounts                        acc,
            bec_dwh.dim_asset_period        dap
        WHERE
                ffaa.book_type_code = acc.book_type_code
            AND ffaa.gl_account = acc.gl_account
            --AND acc.account_type = 'Asset Account'
            AND dap.book_type_code = acc.book_type_code
            AND ffaa.date_effective BETWEEN dap.period_open_date AND dap.period_close_date
        GROUP BY
            ffaa.book_type_code,
            ffaa.gl_account,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            acc.account_type,
            acc.segment3_desc
        UNION
        SELECT
            ffca.book_type_code,
            acc.account_type,
            ffca.gl_account,
            acc.segment3_desc,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            0                begin_balance,
			0              end_balance,
            0                additions,
            SUM(ffca.change) adjustments,
            0                retirement,
            0  depreciation,
            0 retirement_reserve,
            0 transfer,
            0 reclass
        FROM
            bec_dwh.fact_fa_cost_adj ffca,
            accounts                 acc,
            bec_dwh.dim_asset_period dap
        WHERE
                ffca.book_type_code = acc.book_type_code
            AND ffca.gl_account = acc.gl_account
            --AND acc.account_type = 'Asset Account'
            AND dap.book_type_code = acc.book_type_code
            AND ffca.date_effective BETWEEN dap.period_open_date AND dap.period_close_date
        GROUP BY
            ffca.book_type_code,
            acc.account_type,
            ffca.gl_account,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            acc.segment3_desc
        UNION
        SELECT
            ffr.book_type_code,
            acc.account_type,
            ffr.account,
            acc.segment3_desc,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            0             begin_balance,
			0              end_balance,
            0             additions,
            0             adjustments,
            SUM(ffr.cost) retirement,
            0  depreciation,
            0 retirement_reserve,
            0 transfer,
            0 reclass
        FROM
            bec_dwh.fact_fa_retirements ffr,
            accounts                    acc,
            bec_dwh.dim_asset_period    dap
        WHERE
                ffr.book_type_code = acc.book_type_code
            AND ffr.account = acc.gl_account
            --AND acc.account_type = 'Asset Account'
            AND dap.book_type_code = acc.book_type_code
            AND ffr.date_effective BETWEEN dap.period_open_date AND dap.period_close_date
        GROUP BY
            ffr.book_type_code,
            ffr.account,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            acc.account_type,
            acc.segment3_desc
        union 
        SELECT
            ffr.book_type_code,
            acc.account_type,
            acc.segment3,
            acc.segment3_desc,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            0             begin_balance,
			0              end_balance,
            0             additions,
            0             adjustments,
            0             retirement,
            sum(ffr.depreciation)  depreciation,
            sum(ffr.retirement) retirement_reserve,
            sum(transfer) transfer,
            sum(reclass) reclass            
        FROM
            bec_dwh.FACT_FA_ASSET_RESERVES ffr,
            accounts                    acc,
            bec_dwh.dim_asset_period    dap
        WHERE
                ffr.book_type_code = acc.book_type_code
            AND ffr.adjustment_ccid = acc.ccid
            --AND acc.account_type = 'Deprn Reserve Account'
            AND dap.book_type_code = acc.book_type_code
            AND ffr.period_counter_created = dap.period_counter 
        GROUP BY
            ffr.book_type_code,
            acc.segment3,
            dap.fiscal_year,
            dap.period_name,
            dap.period_num,
            acc.account_type,
            acc.segment3_desc
        UNION
        SELECT
            acc.book_type_code,
            acc.account_type,
            ftb.segment3,
            acc.segment3_desc,
            ftb.period_year,
            ftb.period_name,
            ftb.period_num,
            SUM(begin_balance),
			SUM(ending_balance),
            0 additions,
            0 adjustments,
            0 retirement,
            0 depreciation,
            0 retirement_reserve,
            0 transfer,
            0 reclass
        FROM
            bec_dwh.fact_trial_balance ftb,
            accounts                   acc,
            bec_dwh.dim_asset_period   dap
        WHERE
                ftb.ledger_id = acc.set_of_books_id
            AND ftb.segment3 = acc.gl_account
			AND ftb.segment1 = acc.segment1
            AND dap.book_type_code = acc.book_type_code
            AND ftb.period_name = dap.period_name
        GROUP BY
            acc.book_type_code,
            ftb.segment3,
            ftb.period_year,
            ftb.period_name,
            ftb.period_num,
            acc.account_type,
            acc.segment3_desc
    ) 
where 1=1
--and book_type_code = 'BE US CORP BOOK' 
--and gl_account = '13100' --'12701' 
--and fiscal_year = 2022 --and period_name = 'DEC-22';
GROUP BY
    book_type_code,
    account_type,
    gl_account,
    segment3_desc,
    fiscal_year,
    period_name,
    period_num
);

end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_fa_cost_rollup' 
  and batch_name = 'fa';
commit;