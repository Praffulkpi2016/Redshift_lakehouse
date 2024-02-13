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

drop table if exists bec_dwh.FACT_FA_ASSETS_ADDITION;

create table bec_dwh.FACT_FA_ASSETS_ADDITION diststyle all sortkey(transaction_header_id,
DISTRIBUTION_ID)
as
SELECT 
TRANSACTION_HEADER_ID
,(select system_id from bec_etl_ctrl.etlsourceappid
where source_system = 'EBS')|| '-' ||TRANSACTION_HEADER_ID as TRANSACTION_HEADER_ID_KEY
,TRANSACTION_TYPE_CODE
,TRANSACTION_DATE_ENTERED
,INVOICE_TRANSACTION_ID
,MASS_REFERENCE_ID
,LAST_UPDATE_DATE1
,DATE_EFFECTIVE
,SOURCE_TYPE_CODE
,ADJUSTMENT_TYPE
,DEBIT_CREDIT_FLAG
,BOOK_TYPE_CODE
,ASSET_ID
,(select system_id from bec_etl_ctrl.etlsourceappid
where source_system = 'EBS')|| '-' ||ASSET_ID as ASSET_ID_KEY
,ADJUSTMENT_AMOUNT
,DISTRIBUTION_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,ADJUSTMENT_LINE_ID
,ASSET_INVOICE_ID
,SOURCE_DEST_CODE
,PERIOD_COUNTER_CREATED
,CODE_COMBINATION_ID
,(select system_id from bec_etl_ctrl.etlsourceappid
where source_system = 'EBS')|| '-' ||CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY
,LOCATION_ID
,LAST_UPDATE_DATE2
,UNITS_ASSIGNED
,DATE_INEFFECTIVE
,DATE_PLACED_IN_SERVICE
,LIFE_IN_MONTHS
,DEPRN_METHOD
,GRP_ASSET_ID
,PROD
,ADJ_RATE
,BONUS_RATE
,ASSET_TYPE
,GL_ACCOUNT
,RESERVE_ACCT
,COST
,YTD_DEPRN
,DEPRN_RESERVE,
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
	 || '-' || nvl(DISTRIBUTION_ID, 0)
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from 
 (select 
    th.transaction_header_id,
	th.transaction_type_code, 
	th.transaction_date_entered, 
	th.invoice_transaction_id,
	th.mass_reference_id, 
	th.last_update_date last_update_date1,
	th.date_effective,
	adj1.source_type_code, 
	adj1.adjustment_type, 
	adj1.debit_credit_flag,
	adj1.book_type_code, 
	adj1.asset_id,
	adj1.adjustment_amount, 
	adj1.distribution_id,
	adj1.last_update_date, 
	adj1.last_updated_by, 
	adj1.adjustment_line_id,
	adj1.asset_invoice_id,
	adj1.source_dest_code source_dest_code,
	adj1.period_counter_created,
	dh.code_combination_id,
	dh.location_id,
	dh.last_update_date last_update_date2,
	dh.units_assigned units_assigned,
	nvl(dh.date_ineffective, to_date('01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) date_ineffective,
	bks.date_placed_in_service date_placed_in_service,
	bks.life_in_months life_in_months,
	bks.deprn_method_code deprn_method,
	bks.group_asset_id grp_asset_id,
	bks.PRODUCTION_CAPACITY                                           PROD,
    bks.ADJUSTED_RATE                                                 ADJ_RATE,
	decode (AH.ASSET_TYPE, 'CIP', 0,NVL(DS.BONUS_RATE,0))             BONUS_RATE,
	AH.ASSET_TYPE ASSET_TYPE,
	decode(AH.ASSET_TYPE, 'CIP', fcb.CIP_COST_ACCT,fcb.ASSET_COST_ACCT) GL_ACCOUNT,
    decode(AH.ASSET_TYPE, 'CIP', NULL,fcb.DEPRN_RESERVE_ACCT) RESERVE_ACCT,
	nvl(decode(adj1.debit_credit_flag,'DR',1,-1)
       * adj1.ADJUSTMENT_AMOUNT, DD.ADDITION_COST_TO_CLEAR) cost,
	NVL(DD.YTD_DEPRN,0) YTD_DEPRN,
	DD.DEPRN_RESERVE   DEPRN_RESERVE
	FROM 
      (select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y')ah          ,
      (select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') th    ,
      (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') dh   ,
      (select * from bec_ods.FA_ADJUSTMENTS where is_deleted_flg <> 'Y') ADJ1      ,       
      (select * from bec_ods.fa_books where is_deleted_flg <> 'Y') bks             ,
      (select * from bec_ods.fa_deprn_summary where is_deleted_flg <> 'Y') ds      ,
      (select * from bec_ods.FA_DEPRN_DETAIL where is_deleted_flg <> 'Y') DD   ,
       bec_ods.fa_category_books fcb	  
     where
	  1=1
  AND adj1.book_type_code = th.book_type_code  
  AND adj1.transaction_header_id = th.transaction_header_id 
  AND ((adj1.source_type_code = 'CIP ADDITION' and adj1.adjustment_type = 'CIP COST') or
       (adj1.source_type_code = 'ADDITION' and adj1.adjustment_type = 'COST'))
  AND dh.distribution_id = adj1.distribution_id
  AND ah.asset_id                               = th.asset_id
  AND th.date_effective                         >= ah.date_effective 
  AND th.date_effective                         < nvl(ah.date_ineffective, getdate())
  AND bks.transaction_header_id_in              = th.transaction_header_id
  AND dd.book_type_code (+)                     = adj1.book_type_code 
  AND dd.distribution_id (+)                    = adj1.distribution_id 
  AND dd.deprn_source_code (+)                  = 'B'
  AND ds.book_type_code (+)                     = adj1.book_type_code 
  and ds.asset_id (+)                           = adj1.asset_id 
  and ds.period_counter (+)                     = adj1.period_counter_created
  AND fcb.CATEGORY_ID		=	AH.CATEGORY_ID			
  AND   fcb.BOOK_TYPE_CODE	=	TH.BOOK_TYPE_CODE
  UNION 
  SELECT 
    th.transaction_header_id,
	th.transaction_type_code, 
	th.transaction_date_entered, 
	th.invoice_transaction_id,
	th.mass_reference_id, 
	th.last_update_date last_update_date1,
	th.date_effective,
	NULL source_type_code, 
	NULL adjustment_type, 
	NULL debit_credit_flag,
	th.book_type_code, 
	th.asset_id,
	NULL adjustment_amount, 
	NULL distribution_id,
	NULL last_update_date, 
	NULL last_updated_by, 
	NULL adjustment_line_id,
	NULL asset_invoice_id,
	NULL source_dest_code,
	NULL period_counter_created,
	dh.code_combination_id,
	dh.location_id,
	dh.last_update_date last_update_date2,
	dh.units_assigned units_assigned,
	nvl(dh.date_ineffective, to_date('01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) date_ineffective,
	bks.date_placed_in_service date_placed_in_service,
	bks.life_in_months life_in_months,
	bks.deprn_method_code deprn_method,
	bks.group_asset_id grp_asset_id,
	bks.PRODUCTION_CAPACITY                                           PROD,
    bks.ADJUSTED_RATE                                                 ADJ_RATE,
	decode (AH.ASSET_TYPE, 'CIP', 0,NVL(DS.BONUS_RATE,0))             BONUS_RATE,
	AH.ASSET_TYPE ASSET_TYPE,
	decode(AH.ASSET_TYPE, 'CIP', fcb.CIP_COST_ACCT,fcb.ASSET_COST_ACCT) GL_ACCOUNT,
    decode(AH.ASSET_TYPE, 'CIP', NULL,fcb.DEPRN_RESERVE_ACCT) RESERVE_ACCT,
	0 cost,
	0 YTD_DEPRN,
	0  DEPRN_RESERVE
FROM 
      (select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y') ah          ,
      (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') dh   ,      
      (select * from bec_ods.fa_books where is_deleted_flg <> 'Y') bks             ,
      (select * from bec_ods.FA_DEPRN_SUMMARY where is_deleted_flg <> 'Y') DS      ,
      (select th.book_type_code ,
              th.transaction_header_id ,
              th.asset_id ,
              th.date_effective ,
              dp.period_counter ,
			  th.transaction_type_code,
			  th.transaction_date_entered,
			  th.invoice_transaction_id,
				th.mass_reference_id, 
				th.last_update_date 
         from (select * from bec_ods.fa_transaction_headers where is_deleted_flg <> 'Y') th ,
              (select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') dp       
        where
              th.transaction_type_code in ('ADDITION', 'CIP ADDITION')             
          AND dp.book_type_code = th.book_type_code and
              th.date_effective between dp.period_open_date and 
                                   nvl (dp.period_close_date, getdate())
      ) th,
	  bec_ods.fa_category_books fcb
WHERE 
      dh.asset_id = th.asset_id 
  and th.date_effective                         >= dh.date_effective 
  and th.date_effective                         < nvl(dh.date_ineffective, getdate())
  AND ah.asset_id                               = th.asset_id 
  and th.date_effective                        >= ah.date_effective 
  and th.date_effective                         < nvl(ah.date_ineffective, getdate())
  AND bks.transaction_header_id_in               = th.transaction_header_id 
  and bks.cost = 0
  AND ds.book_type_code (+)                     = th.book_type_code 
  and ds.asset_id (+)                           = th.asset_id 
  and ds.period_counter (+)                     = th.period_counter
  AND fcb.CATEGORY_ID		=	AH.CATEGORY_ID			
  AND   fcb.BOOK_TYPE_CODE	=	TH.BOOK_TYPE_CODE
  )
;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_fa_assets_addition'
	and batch_name = 'fa';

commit;