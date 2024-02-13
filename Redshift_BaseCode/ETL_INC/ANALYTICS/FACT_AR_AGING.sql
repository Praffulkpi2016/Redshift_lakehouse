 /*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental approach for Facts.
# File Version: KPI v1.0
*/

begin;

TRUNCATE TABLE bec_dwh.FACT_AR_AGING;

INSERT INTO bec_dwh.FACT_AR_AGING 
(select 
customer_id
,customer_site_use_id
,(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||CUSTOMER_SITE_USE_ID AS CUSTOMER_SITE_USE_ID_KEY
,customer_trx_id
,payment_schedule_id
,class
,primary_salesrep_id
,batch_source_id
,due_date
,acct_amount_due_remaining
,trns_amount_due_remaining
,trx_number
,amount_adjusted
,amount_applied
,AMOUNT_DUE_ORIGINAL
,amount_credited
,amount_adjusted_pending
,gl_date
,gl_date_closed
,cust_trx_type_id
,(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||CUST_TRX_TYPE_ID AS CUST_TRX_TYPE_ID_KEY
,org_id
,(SELECT SYSTEM_ID FROM BEC_ETL_CTRL.ETLSOURCEAPPID 
WHERE SOURCE_SYSTEM='EBS')||'-'||ORG_ID AS ORG_ID_KEY
,invoice_currency_code
,source_name
,exchange_rate
,GBL_TOTAL_AMOUNT
,GBL_AMOUNT_DUE_REMAINING
,'N' as IS_DELETED_FLG
,				(
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
    || '-' || nvl(customer_trx_id, 0)
	|| '-' || nvl(payment_schedule_id, 0) 
    || '-' || nvl(customer_site_use_id, 0) 
	|| '-' || nvl(source_name,'NA')
	as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
FROM
(
select a.customer_id,
      a.customer_site_use_id ,
      a.customer_trx_id,
      a.payment_schedule_id,
      a.class ,
      sum(a.primary_salesrep_id) primary_salesrep_id,
	  a.batch_source_id,
      a.due_date ,
      sum(a.acct_amount_due_remaining) acct_amount_due_remaining,
	  sum(a.trns_amount_due_remaining) trns_amount_due_remaining,
      a.trx_number,
      a.amount_adjusted,
      a.amount_applied ,
	  a.AMOUNT_DUE_ORIGINAL,
      a.amount_credited ,
      a.amount_adjusted_pending,
      a.gl_date ,
	  a.gl_date_closed,
      a.cust_trx_type_id,
      a.org_id,
      a.invoice_currency_code,
	  a.source_name,
      a.exchange_rate	 ,
(a.GBL_TOTAL_AMOUNT) as GBL_TOTAL_AMOUNT,
sum(a.GBL_AMOUNT_DUE_REMAINING)	as GBL_AMOUNT_DUE_REMAINING  
from 
(  select  
      ps.customer_id,
      ps.customer_site_use_id ,
      ps.customer_trx_id,
      ps.payment_schedule_id,
      ps.class ,
      0 primary_salesrep_id,
	  ct.batch_source_id,
      ps.due_date ,
	  sum(nvl(adj.acctd_amount, 0)) as acct_amount_due_remaining,
	  sum(nvl(adj.amount, 0)) as trns_amount_due_remaining,
      ps.trx_number,
      ps.amount_adjusted ,
      ps.amount_applied ,
	  ps.AMOUNT_DUE_ORIGINAL,
      ps.amount_credited ,
      ps.amount_adjusted_pending,
      ps.gl_date ,
	  ps.gl_date_closed,
      ps.cust_trx_type_id,
      ps.org_id,
      ps.invoice_currency_code,
	  ra_batch_sources_all.name AS source_name,
      nvl(ps.exchange_rate,1) exchange_rate,
	   cast(NVL(ps.AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_TOTAL_AMOUNT,
    cast(NVL(adj.amount,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_DUE_REMAINING 
                from 
                  (select * from bec_ods.ar_payment_schedules_all where is_deleted_flg <> 'Y') ps, 
                  (select * from bec_ods.ar_adjustments_all where is_deleted_flg <> 'Y') adj, 
                  (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') ct, 
                  (select * from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y') ra_batch_sources_all, 
                  (
                    select 
                      * 
                    from 
                      (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') 
                    where 
                      to_currency = 'USD' 
                      and conversion_type = 'Corporate'
                  ) DCR 
                where 
                  1 = 1 
                  and adj.payment_schedule_id = ps.payment_schedule_id 
                  and adj.status = 'A' 
                  and ps.customer_id > 0 
                  and ps.customer_trx_id = ct.customer_trx_id(+) 
                  and PS.invoice_currency_code = DCR.from_currency(+) 
                  and DCR.conversion_date(+) = ps.gl_date 
                  and ct.batch_source_id = ra_batch_sources_all.batch_source_id(+) 
                  and ct.org_id = ra_batch_sources_all.org_id(+)  
 group by 
   ps.customer_id,
      ps.customer_site_use_id ,
      ps.customer_trx_id,
      ps.class ,
	  ct.batch_source_id,
      ps.due_date,
      ps.trx_number,
      ps.amount_adjusted ,
      ps.amount_applied ,
	  ps.AMOUNT_DUE_ORIGINAL,
      ps.amount_credited ,
      ps.amount_adjusted_pending,
      ps.gl_date ,
	  ps.gl_date_closed,
      ps.cust_trx_type_id,
      ps.org_id,
      ps.invoice_currency_code,
	  ra_batch_sources_all.name,
      nvl(ps.exchange_rate,1),
      ps.payment_schedule_id,
	 cast(NVL(ps.AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) ,
    cast(NVL(adj.amount,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) 
UNION ALL
   select  ps.customer_id,
      ps.customer_site_use_id ,
      ps.customer_trx_id,
      ps.payment_schedule_id,
      ps.class ,
      0 primary_salesrep_id,
	  ct.batch_source_id,
      ps.due_date  ,
		nvl(sum (     (decode(ps.class, 'CM',
				decode ( app.application_type, 'CM',
					 app.acctd_amount_applied_from,
                                         app.acctd_amount_applied_to
					),
				app.acctd_amount_applied_to)+
                       nvl(app.acctd_earned_discount_taken,0) +
                       nvl(app.acctd_unearned_discount_taken,0))
					   *
		   decode
                   ( ps.class, 'CM',
                      decode(app.application_type, 'CM', -1, 1), 1 )),0)
	  as acct_amount_due_remaining,	  
nvl(sum( ( app.amount_applied +
                       nvl(app.earned_discount_taken,0) +
                       nvl(app.unearned_discount_taken,0) )
					   *
		   decode
                   ( ps.class, 'CM',
                      decode(app.application_type, 'CM', -1, 1), 1 )),0)
					  					   as trns_amount_due_remaining,  
      ps.trx_number ,
      ps.amount_adjusted,
      ps.amount_applied ,
	  ps.AMOUNT_DUE_ORIGINAL,
      ps.amount_credited ,
      ps.amount_adjusted_pending,
      ps.gl_date gl_date_inv,
	  ps.gl_date_closed,
      ps.cust_trx_type_id,
      ps.org_id,
      ps.invoice_currency_code, 
	  ra_batch_sources_all.name AS source_name,
      nvl(ps.exchange_rate, 1) exchange_rate,
	  cast(NVL(ps.AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_TOTAL_AMOUNT,
    cast(NVL(sum( ( app.amount_applied +
                       nvl(app.earned_discount_taken,0) +
                       nvl(app.unearned_discount_taken,0) )
					   *
		   decode
                   ( ps.class, 'CM',
                      decode(app.application_type, 'CM', -1, 1), 1 )),0) * NVL(DCR.conversion_rate,1)
                      as decimal(18,2)) GBL_AMOUNT_DUE_REMAINING
               from 
                  (select * from bec_ods.ar_payment_schedules_all where is_deleted_flg <> 'Y') ps, 
                  (select * from bec_ods.ar_receivable_applications_all where is_deleted_flg <> 'Y') app, 
                  (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') ct, 
                  (select * from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y') ra_batch_sources_all, 
                  (
                    select 
                      * 
                    from 
                      (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') 
                    where 
                      to_currency = 'USD' 
                      and conversion_type = 'Corporate'
                  ) DCR 
                where 
                  1 = 1 
                  and ps.customer_id > 0 
                  and (
                    app.applied_payment_schedule_id = ps.payment_schedule_id 
                    OR app.payment_schedule_id = ps.payment_schedule_id
                  ) 
                  and app.status = 'APP' 
                  and nvl(app.confirmed_flag, 'Y') = 'Y' 
                  and ps.customer_trx_id = ct.customer_trx_id(+) 
                  and PS.invoice_currency_code = DCR.from_currency(+) 
                  and DCR.conversion_date(+) = ps.gl_date 
                  and ct.batch_source_id = ra_batch_sources_all.batch_source_id(+) 
                  and ct.org_id = ra_batch_sources_all.org_id(+) 
  group by 
  ps.customer_id,
      ps.customer_site_use_id ,
      ps.customer_trx_id,
      ps.class ,
	  ct.batch_source_id,
      ps.due_date,
      ps.trx_number,
      ps.amount_adjusted ,
      ps.amount_applied ,
	  ps.AMOUNT_DUE_ORIGINAL,
      ps.amount_credited ,
      ps.amount_adjusted_pending,
      ps.gl_date ,
	  ps.gl_date_closed,
      ps.cust_trx_type_id,
      ps.org_id,
      ps.invoice_currency_code,
	  ra_batch_sources_all.name,
      nvl(ps.exchange_rate, 1),
      ps.payment_schedule_id,
      dcr.conversion_rate
UNION ALL
   select  ps.customer_id,
      ps.customer_site_use_id ,
      ps.customer_trx_id,
      ps.payment_schedule_id,
      ps.class class_inv,
      nvl(ct.primary_salesrep_id, 0) primary_salesrep_id,
	  ct.batch_source_id,
      ps.due_date  due_date_inv,
	  ps.acctd_amount_due_remaining as acct_amount_due_remaining,
	  ps.amount_due_remaining as trns_amount_due_remaining,
      ps.trx_number,
      ps.amount_adjusted ,
      ps.amount_applied ,
	  ps.AMOUNT_DUE_ORIGINAL,
      ps.amount_credited ,
      ps.amount_adjusted_pending,
      ps.gl_date ,
	  ps.gl_date_closed,
      ps.cust_trx_type_id,
      ps.org_id,
      ps.invoice_currency_code,
	  ra_batch_sources_all.name AS source_name,
      nvl(ps.exchange_rate, 1) exchange_rate,
	  cast(NVL(ps.AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_TOTAL_AMOUNT,
    cast(NVL(ps.amount_due_remaining,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_DUE_REMAINING
   from  (SELECT * FROM bec_ods.ar_payment_schedules_all where is_deleted_flg <> 'Y') ps,
         (SELECT * FROM bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') ct,
		 (SELECT * FROM bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y') ra_batch_sources_all,
		 (select * from 
bec_ods.GL_DAILY_RATES 
where 
to_currency = 'USD'
and conversion_type = 'Corporate'
AND is_deleted_flg <> 'Y') DCR
   where  1=1
     and ps.customer_id > 0
    and  ps.customer_trx_id = ct.customer_trx_id(+)
	and PS.invoice_currency_code = DCR.from_currency(+)
and DCR.conversion_date(+) = ps.gl_date
and ct.batch_source_id = ra_batch_sources_all.batch_source_id(+)
and ct.org_id = ra_batch_sources_all.org_id(+)
  UNION ALL
   select  ps.customer_id,
      ps.customer_site_use_id ,
      ps.customer_trx_id,
      ps.payment_schedule_id,
      ps.class class_inv,
      ct.primary_salesrep_id ,
	  ct.batch_source_id,
      ps.due_date  due_date_inv,
	 ps.acctd_amount_due_remaining as acct_amount_due_remaining,
	  ps.amount_due_remaining as trns_amount_due_remaining,  
      ps.trx_number,
      ps.amount_adjusted ,
      ps.amount_applied ,
	  ps.AMOUNT_DUE_ORIGINAL,
      ps.amount_credited ,
      ps.amount_adjusted_pending,
      ps.gl_date ,
	  ps.gl_date_closed,
      ps.cust_trx_type_id,
      ps.org_id,
      ps.invoice_currency_code,
	  ra_batch_sources_all.name AS source_name,
      nvl(ps.exchange_rate, 1) exchange_rate,
	  cast(NVL(ps.AMOUNT_DUE_ORIGINAL,0) * NVL(DCR.conversion_rate,1) as decimal(18,2)) GBL_TOTAL_AMOUNT,
    cast(NVL(ps.amount_due_remaining,0) * NVL(DCR.conversion_rate,1)as decimal(18,2)) GBL_AMOUNT_DUE_REMAINING
              from 
                  (select * from bec_ods.ar_payment_schedules_all where is_deleted_flg <> 'Y') ps, 
                  (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') ct, 
                  (select * from bec_ods.ar_adjustments_all where is_deleted_flg <> 'Y') adj, 
                  (select * from bec_ods.ra_batch_sources_all where is_deleted_flg <> 'Y') ra_batch_sources_all, 
                  (
                    select 
                      * 
                    from 
                      (select * from bec_ods.GL_DAILY_RATES where is_deleted_flg <> 'Y') 
                    where 
                      to_currency = 'USD' 
                      and conversion_type = 'Corporate'
                  ) DCR 
                where 
                  1 = 1 
                  and ps.customer_id > 0 
                  and ps.class = 'CB' 
                  and ps.customer_trx_id = adj.chargeback_customer_trx_id 
                  and ps.customer_trx_id = ct.customer_trx_id(+) 
                  and PS.invoice_currency_code = DCR.from_currency(+) 
                  and DCR.conversion_date(+) = ps.gl_date 
                  and ct.batch_source_id = ra_batch_sources_all.batch_source_id(+) 
                  and ct.org_id = ra_batch_sources_all.org_id(+)
	)a
	group by 
	a.customer_id,
      a.customer_site_use_id ,
      a.customer_trx_id,
      a.payment_schedule_id,
      a.class ,
	  a.batch_source_id,
      a.due_date ,
      a.trx_number,
      a.amount_adjusted,
      a.amount_applied ,
	  a.AMOUNT_DUE_ORIGINAL,
      a.amount_credited ,
      a.amount_adjusted_pending,
      a.gl_date ,
	  a.gl_date_closed,
      a.cust_trx_type_id,
      a.org_id,
      a.invoice_currency_code,
	  a.source_name,
      a.exchange_rate,
	  a.GBL_TOTAL_AMOUNT
)
);

END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ar_aging'
	and batch_name = 'ar';
	
commit;
