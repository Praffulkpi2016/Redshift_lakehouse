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
truncate table bec_dwh.FACT_FA_COST_ADJ;
insert into bec_dwh.FACT_FA_COST_ADJ ( 
select 
asset_type,
asset_id,
book_type_code,
thid,
category_id,
code_combination_id,
DATE_EFFECTIVE,
GL_ACCOUNT,
DEPRN_RESERVE_ACCT,
GL_ACCOUNT_CCID,
OLD_COST,
NEW_COST,
CHANGE,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || asset_id as asset_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || category_id as category_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || code_combination_id as code_combination_id_key,
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
	 || '-' || nvl(thid, 0)   || '-' || nvl(code_combination_id, 0)
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
SELECT
    asset_type,
    asset_id,
    book_type_code,
    thid,
    category_id,
    code_combination_id,
	DATE_EFFECTIVE,
	GL_ACCOUNT,
	DEPRN_RESERVE_ACCT,
	GL_ACCOUNT_CCID,
    SUM(decode(unit_sum, units, old_cost1 + old_cost - old_cost_rsum, old_cost1)) OLD_COST,
    SUM(decode(unit_sum, units, new_cost1 + new_cost - new_cost_rsum, new_cost1)) NEW_COST,
    SUM(decode(unit_sum, units, new_cost1 + new_cost - new_cost_rsum, new_cost1) -
       	decode(unit_sum, units, old_cost1 + old_cost - old_cost_rsum, old_cost1)) CHANGE
FROM
    (
        SELECT
            ah.asset_type asset_type,
            ah.asset_id,
            th.book_type_code,
            th.transaction_header_id  thid,
            round((books_old.cost * nvl(dh.units_assigned, ah.units) / ah.units),4) old_cost1,
            (round((books_old.cost * nvl(dh.units_assigned, ah.units) / ah.units),4) + 
			round(((books_new.cost - books_old.cost) * nvl(dh.units_assigned, ah.units)/ah.units),4)) new_cost1,
            SUM(round((books_old.cost * nvl(dh.units_assigned, ah.units) / ah.units),4))
            OVER(PARTITION BY th.transaction_header_id,dh.asset_id ) old_cost_rsum,
            SUM((round((books_old.cost * nvl(dh.units_assigned, ah.units) / ah.units),4) + 
			round(((books_new.cost - books_old.cost) * nvl(dh.units_assigned, ah.units) / ah.units),4)))
            OVER(PARTITION BY th.transaction_header_id, dh.asset_id ) new_cost_rsum,
            SUM(nvl(dh.units_assigned, ah.units)) OVER(PARTITION BY th.transaction_header_id,dh.asset_id 
			 ) unit_sum,
            ah.units         units,
            books_old.cost   old_cost,
            books_new.cost   new_cost,
            ah.category_id,
            dh.code_combination_id,
			TH.DATE_EFFECTIVE,
			decode(ah.ASSET_TYPE, 'CIP', fcb.CIP_COST_ACCT,fcb.ASSET_COST_ACCT) GL_ACCOUNT,
            decode(ah.ASSET_TYPE, 'CIP', NULL,fcb.DEPRN_RESERVE_ACCT)  DEPRN_RESERVE_ACCT,
			decode(ah.ASSET_TYPE, 'CIP',fcb.WIP_COST_ACCOUNT_CCID,fcb.ASSET_COST_ACCOUNT_CCID) GL_ACCOUNT_CCID
        FROM
            (select * from bec_ods.fa_asset_history        where is_deleted_flg <> 'Y')ah,
            (select * from bec_ods.fa_books                where is_deleted_flg <> 'Y')books_old,
            (select * from bec_ods.fa_books                where is_deleted_flg <> 'Y')books_new,
            (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y')dh,
            (select * from bec_ods.fa_transaction_headers  where is_deleted_flg <> 'Y')th,
			(select * from bec_ods.fa_category_books where is_deleted_flg <> 'Y')fcb
        WHERE
                1 = 1
            AND th.transaction_type_code IN ( 'ADJUSTMENT', 'CIP ADJUSTMENT' )
            AND books_old.transaction_header_id_out = th.transaction_header_id
            AND books_old.book_type_code = th.book_type_code
            AND books_new.transaction_header_id_in = th.transaction_header_id
            AND books_new.book_type_code = th.book_type_code
            AND ah.asset_id = th.asset_id            
            AND th.transaction_header_id >= ah.transaction_header_id_in
            AND th.transaction_header_id < nvl(ah.transaction_header_id_out, th.transaction_header_id + 1)
            AND th.asset_id = dh.asset_id
            AND th.book_type_code = dh.book_type_code
            AND th.transaction_header_id >= dh.transaction_header_id_in
            AND th.transaction_header_id < nvl(dh.transaction_header_id_out, th.transaction_header_id + 1)
            AND round((books_old.cost * nvl(dh.units_assigned, ah.units) / ah.units),4) != 
			    round((books_new.cost * nvl(dh.units_assigned, ah.units) / ah.units),4)
			AND fcb.CATEGORY_ID		=	AH.CATEGORY_ID			
	        AND   fcb.BOOK_TYPE_CODE	=	TH.BOOK_TYPE_CODE
    ) 
GROUP BY
    asset_type,
    asset_id,
    book_type_code,
    thid,
    category_id,
    code_combination_id,
    DATE_EFFECTIVE,
    GL_ACCOUNT,
	DEPRN_RESERVE_ACCT,
	GL_ACCOUNT_CCID
)
);

END;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_fa_cost_adj' 
  and batch_name = 'fa';
commit;
