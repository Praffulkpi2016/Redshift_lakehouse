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
BEGIN;
-- Delete Records
	DELETE
FROM
	bec_dwh.FACT_REVENUE_AMORTIZATION
WHERE
	(
	cast(nvl(site_id, '0') as varchar(2000)),
	nvl(ledger_id,0),
	nvl(CONTRACT_ID,0),
	nvl(customer_trx_line_id,0),
	nvl(ACTUAL_FORECAST,'0'),
    nvl(REVENUE_TYPE,'0'),
	nvl(period_name, '0'),
	nvl(start_date, '1900-01-01 12:00:00'),
    nvl(end_date, '1900-01-01 12:00:00')
	) IN 
(
SELECT
		cast(nvl(ODS.site_id, '0') as varchar(2000)) as site_id,
		nvl(ODS.ledger_id,0) as ledger_id,
		nvl(ODS.contract_id,0) as contract_id,
		nvl(ODS.customer_trx_line_id,0) as customer_trx_line_id,
		nvl(ODS.ACTUAL_FORECAST,'0') as ACTUAL_FORECAST,
		nvl(ODS.REVENUE_TYPE,'0') as REVENUE_TYPE,
		nvl(ODS.period_name, '0') as period_name,
		nvl(ODS.start_date, '1900-01-01 12:00:00') as start_date,
		nvl(ODS.end_date, '1900-01-01 12:00:00') as end_date
FROM
bec_dwh.FACT_REVENUE_AMORTIZATION dw,
(select 
	master.ledger_id,
	master.site_id,
	serv_rev.customer_trx_line_id,
	serv_rev.ACTUAL_FORECAST,
	serv_rev.CONTRACT_ID,
	serv_rev.PERIOD_NAME,
	serv_rev.REVENUE_TYPE,
	master.start_date,
	master.end_date
from
( SELECT DISTINCT 
	source,
	contract_group,
	ledger_name,
	ledger_id,
	org_name,
	org_id,
	site_id,
	contract_id,
	start_date,
	end_date,
	last_update_date,
	kca_seq_date,
	IS_DELETED_FLG
	FROM
	bec_ods.xxbec_srvcrev_cdw_master_stg 
) master,
(select distinct
		accounted_currency,
		actual_forecast,
		contract_group,
		contract_id,
		-- contract_line_id,
		conversion_rate,
		customer_trx_line_id,
		event_id,
		extract_date,
		from_date,
		gl_dist_id,
		last_update_date,
		ledger_id,
		ledger_name,
		no_days,
		org_id,
		org_name,
		period_end_date,
		period_name,
		query_name,
		revenue_amount_acctd_curr,
		revenue_amount_trans_curr,
		revenue_type,
		site_id,
		"source",
		to_date,
		transaction_currency
		from (
	select distinct
		accounted_currency,
		actual_forecast,
		contract_group,
		contract_id,
		--contract_line_id,
		conversion_rate,
		customer_trx_line_id,
		event_id,
		extract_date,
		from_date,
		gl_dist_id,
		last_update_date,
		ledger_id,
		ledger_name,
		no_days,
		org_id,
		org_name,
		period_end_date,
		period_name,
		query_name,
		revenue_amount_acctd_curr,
		revenue_amount_trans_curr,
		revenue_type,
		site_id,
		"source",
		to_date,
		transaction_currency
	from
		bec_ods.xxbec_srvcrev_cdw_fixed_stg xf 
union all
	select distinct
		accounted_currency,
		actual_forecast,
		contract_group,
		contract_id,
		-- contract_line_id,
		conversion_rate,
		customer_trx_line_id,
		event_id,
		extract_date,
		from_date,
		gl_dist_id,
		last_update_date,
		ledger_id,
		ledger_name,
		no_days,
		org_id,
		org_name,
		period_end_date,
		period_name,
		query_name,
		revenue_amount_acctd_curr,
		revenue_amount_trans_curr,
		revenue_type,
		site_id,
		"source",
		to_date,
		transaction_currency
	from
		bec_ods.xxbec_srvcrev_cdw_variable_stg xv )
     ) serv_rev
where
	master.ledger_id = serv_rev.ledger_id
	and master.site_id = serv_rev.site_id
	and master.contract_id = serv_rev.contract_id 
	AND ( master.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_revenue_amortization'
			AND batch_name = 'ar')
		OR master.is_deleted_flg = 'Y'	)		
group by 
	master.ledger_id,
	master.site_id,
	serv_rev.customer_trx_line_id,
	serv_rev.ACTUAL_FORECAST,
	serv_rev.CONTRACT_ID,
	serv_rev.PERIOD_NAME,
	serv_rev.REVENUE_TYPE,
	master.start_date,
	master.end_date
	order by
	--master.source,
	--master.CONTRACT_GROUP,
	master.Ledger_id,
	master.SITE_ID
) ODS
WHERE
		dw.dw_load_id = 
(
		SELECT
			system_id
		FROM
			bec_etl_ctrl.etlsourceappid
		WHERE
			source_system = 'EBS')    
    || '-' || cast(nvl(ODS.site_id, '0') as varchar(2000))
    || '-' || nvl(ODS.ledger_id,0)
    || '-' || nvl(ODS.contract_id,0)
	|| '-' || nvl(ODS.customer_trx_line_id,0)
	|| '-' || nvl(ODS.ACTUAL_FORECAST,'0')
	|| '-' || nvl(ODS.REVENUE_TYPE,'0')
	|| '-' || nvl(ODS.period_name,'0')
	|| '-' || nvl(ODS.start_date, '1900-01-01 12:00:00')
    || '-' || nvl(ODS.end_date, '1900-01-01 12:00:00')

);
COMMIT;
-- Insert records

INSERT
	INTO
	bec_dwh.FACT_REVENUE_AMORTIZATION
(
    "source",
	contract_group,
	ledger_name,
	ledger_id,
	operating_unit,
	org_id,
	site_id,
	start_date,
	end_date,
	customer_trx_line_id,
	actual_forecast,
	contract_id,
	conversion_rate,
	period_end_date,
	no_days,
	from_date,
	to_date,
	period_name,
	revenue_amount_trans_curr,
	revenue_amount_acctd_curr,
	revenue_type,
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select 
	master.source,
	master.contract_group,
	master.ledger_name,
	master.ledger_id,
	master.org_name Operating_Unit,
	master.org_id,
	master.site_id,
	master.start_date,
	master.end_date,
	serv_rev.customer_trx_line_id,
	serv_rev.ACTUAL_FORECAST,
	serv_rev.CONTRACT_ID,
	serv_rev.CONVERSION_RATE,
	serv_rev.PERIOD_END_DATE,
--	serv_rev.NO_DAYS,
	sum(serv_rev.NO_DAYS) as NO_DAYS,
	min(serv_rev.FROM_DATE) as FROM_DATE,
	max(serv_rev.TO_DATE) as TO_DATE,
	serv_rev.PERIOD_NAME,
	--serv_rev.REVENUE_AMOUNT_ACCTD_CURR,
	cast(sum(serv_rev.revenue_amount_acctd_curr) as decimal(32,2)) as REVENUE_AMOUNT_ACCTD_CURR,
	--serv_rev.REVENUE_AMOUNT_TRANS_CURR,
    cast(sum(serv_rev.revenue_amount_trans_curr) as decimal(32,2)) as REVENUE_AMOUNT_TRANS_CURR,	
	serv_rev.REVENUE_TYPE,
	master.last_update_date,
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
  || '-' || cast(nvl(master.site_id, '0') as varchar(2000)) 
  || '-' || nvl(master.ledger_id, 0)
  || '-' || nvl(serv_rev.CONTRACT_ID,0)
  || '-' || nvl(serv_rev.customer_trx_line_id,0)
  || '-' || nvl(serv_rev.ACTUAL_FORECAST,'0')
  || '-' || nvl(serv_rev.REVENUE_TYPE,'0')
  || '-' || nvl(serv_rev.period_name, '0') 
  || '-' || nvl(master.start_date, '1900-01-01 12:00:00')
  || '-' || nvl(master.end_date, '1900-01-01 12:00:00')
  as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	( SELECT DISTINCT 
	source,
	contract_group,
	ledger_name,
	ledger_id,
	org_name,
	org_id,
	site_id,
	contract_id,
	start_date,
	end_date,
	last_update_date,
	kca_seq_date
	FROM
	 bec_ods.xxbec_srvcrev_cdw_master_stg where is_deleted_flg <> 'Y'
	) master,
	(select distinct
		accounted_currency,
		actual_forecast,
		contract_group,
		contract_id,
		-- contract_line_id,
		conversion_rate,
		customer_trx_line_id,
		event_id,
		extract_date,
		from_date,
		gl_dist_id,
		last_update_date,
		ledger_id,
		ledger_name,
		no_days,
		org_id,
		org_name,
		period_end_date,
		period_name,
		query_name,
		revenue_amount_acctd_curr,
		revenue_amount_trans_curr,
		revenue_type,
		site_id,
		"source",
		to_date,
		transaction_currency
		from (
	select distinct
		accounted_currency,
		actual_forecast,
		contract_group,
		contract_id,
		--contract_line_id,
		conversion_rate,
		customer_trx_line_id,
		event_id,
		extract_date,
		from_date,
		gl_dist_id,
		last_update_date,
		ledger_id,
		ledger_name,
		no_days,
		org_id,
		org_name,
		period_end_date,
		period_name,
		query_name,
		revenue_amount_acctd_curr,
		revenue_amount_trans_curr,
		revenue_type,
		site_id,
		"source",
		to_date,
		transaction_currency
	from
		(select * from bec_ods.xxbec_srvcrev_cdw_fixed_stg where is_deleted_flg <> 'Y') xf
union all
	select distinct
		accounted_currency,
		actual_forecast,
		contract_group,
		contract_id,
		-- contract_line_id,
		conversion_rate,
		customer_trx_line_id,
		event_id,
		extract_date,
		from_date,
		gl_dist_id,
		last_update_date,
		ledger_id,
		ledger_name,
		no_days,
		org_id,
		org_name,
		period_end_date,
		period_name,
		query_name,
		revenue_amount_acctd_curr,
		revenue_amount_trans_curr,
		revenue_type,
		site_id,
		"source",
		to_date,
		transaction_currency
	from
		(select * from bec_ods.xxbec_srvcrev_cdw_variable_stg where is_deleted_flg <> 'Y') xv)
     ) serv_rev
where
	master.ledger_id = serv_rev.ledger_id
	and master.site_id = serv_rev.site_id
	and master.contract_id = serv_rev.contract_id --added to handle duplicates 
    AND (master.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_revenue_amortization'
			AND batch_name = 'ar')	)
group by master.source,
	master.contract_group,
	master.ledger_name,
	master.ledger_id,
	master.org_name,
	master.org_id,
	master.site_id,
	master.start_date,
	master.end_date,
	serv_rev.customer_trx_line_id,
	serv_rev.ACTUAL_FORECAST,
	serv_rev.CONTRACT_ID,
	serv_rev.CONVERSION_RATE,
	serv_rev.PERIOD_END_DATE,
	serv_rev.PERIOD_NAME,
	serv_rev.REVENUE_TYPE,
	master.last_update_date
	order by
	master.source,
	master.CONTRACT_GROUP,
	master.Ledger_id,
	master.SITE_ID
 );
 
 commit;
 
END;

UPDATE
	bec_etl_ctrl.batch_dw_info
SET
	load_type = 'I',
	last_refresh_date = getdate()
WHERE
	dw_table_name = 'fact_revenue_amortization'
	AND batch_name = 'ar';

COMMIT;