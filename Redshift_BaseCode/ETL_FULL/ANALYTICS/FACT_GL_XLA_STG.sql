/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for TEMPORARY STAGING TABLE.
# File Version: KPI v1.0
*/
begin;

-- Temp1
drop table if exists bec_dwh.cte_xla_ae_headers;
create table bec_dwh.CTE_xla_ae_headers as (
	Select 
	 aeh.accounting_date as GL_DATE
    ,aeh.creation_date as CREATION_DATE
    ,aeh.last_update_date as LAST_UPDATE_DATE
    ,aeh.gl_transfer_date as GL_TRANSFER_DATE
    ,aeh.reference_date as REFERENCE_DATE
    ,aeh.completed_date as COMPLETED_DATE
    ,aeh.completion_acct_seq_value ACCOUNTING_SEQUENCE_NUMBER
    ,aeh.close_acct_seq_value REPORTING_SEQUENCE_NUMBER
    ,aeh.doc_sequence_value DOCUMENT_SEQUENCE_NUMBER
    ,aeh.application_id APPLICATION_ID
    ,aeh.ae_header_id HEADER_ID
    ,aeh.description HEADER_DESCRIPTION
    ,aeh.funds_status_code FUND_STATUS
	,aeh.ae_header_id
    ,aeh.je_category_name
    ,aeh.Event_type_code Event_TYPE_CODE
    ,aeh.accounting_entry_status_code
    ,aeh.gl_transfer_status_code
    ,aeh.balance_type_code
    ,aeh.budget_version_id
    ,aeh.Event_id
    ,aeh.entity_id
    ,aeh.completion_acct_seq_version_id
    ,aeh.close_acct_seq_version_id
    ,aeh.doc_sequence_id
	From (select * from bec_ods.xla_ae_headers where is_deleted_flg <> 'Y'
	and accounting_entry_status_code = 'F'
    AND gl_transfer_status_code = 'Y'
    and last_update_date >= to_date('2020-01-01 12:00:00','YYYY-MM-DD HH:MI:SS')
	) aeh
);
commit;
--Temp2
drop table if exists  bec_dwh.cte_xla_ae_lines;
create table bec_dwh.cte_xla_ae_lines as (
	Select ael.gl_sl_link_table
	,ael.application_id
	,ael.ae_header_id
    ,ael.gl_sl_link_id
    ,ael.encumbrance_type_id
    ,ael.accounting_class_code
    ,ael.currency_conversion_type
    ,ael.ae_header_id as ael_ae_header_id
    ,ael.ae_line_num
    ,ael.displayed_line_number LINE_NUMBER
    ,ael.ae_line_num ORIG_LINE_NUMBER
    ,ael.description LINE_DESCRIPTION
    ,ael.currency_code ENTERED_CURRENCY
    ,ael.currency_conversion_rate CONVERSION_RATE
    ,ael.currency_conversion_date as CONVERSION_RATE_DATE
    ,ael.currency_conversion_type CONVERSION_RATE_TYPE_CODE
    ,ael.ENTERED_dr ENTERED_DR
    ,ael.ENTERED_cr ENTERED_CR
    ,ael.unrounded_accounted_dr UNROUNDED_ACCOUNTED_DR
    ,ael.unrounded_accounted_cr UNROUNDED_ACCOUNTED_CR
    ,ael.statistical_amount STATISTICAL_AMOUNT
    ,ael.jgzz_recon_ref RECONCILIATION_REFERENCE
    ,ael.party_type_code PARTY_TYPE_CODE
	From (select * from bec_ods.xla_ae_lines where is_deleted_flg <> 'Y'
	and last_update_date >= to_date('2020-01-01 12:00:00','YYYY-MM-DD HH:MI:SS')
	) ael
);
commit;
drop table if exists bec_dwh.cte_xla_event_types_tl;
create table bec_dwh.cte_xla_event_types_tl as (
	Select xet.Event_class_code Event_CLASS_CODE
    ,xet.NAME Event_TYPE_NAME
    ,xet.entity_code
	,xet.application_id 
	,xet.Event_type_code
	From (select * from bec_ods.xla_Event_types_tl where is_deleted_flg <> 'Y'
	and LANGUAGE = 'US'
	) xet
);

commit;

drop table if exists bec_dwh.CTE_xla_Event_classes_tl;
create table bec_dwh.CTE_xla_Event_classes_tl as (
	Select xect.NAME Event_CLASS_NAME
	,xect.application_id
	,xect.entity_code
	,xect.Event_class_code
	From (select * from bec_ods.xla_Event_classes_tl where is_deleted_flg <> 'Y'
	and LANGUAGE = 'US'
	) xect
);

commit;

drop table if exists bec_dwh.cte_xla_events ;
create table bec_dwh.cte_xla_events as (
	Select 
	xle.transaction_date
    ,xle.Event_id as xle_Event_ID
    ,xle.Event_date
    ,xle.Event_number Event_NUMBER
	,xle.application_id
	,xle.Event_id
	From (select * from bec_ods.xla_Events where is_deleted_flg <> 'Y'
	and last_update_date >= to_date('2020-01-01 12:00:00','YYYY-MM-DD HH:MI:SS')
	) xle
);
commit;

drop table if exists bec_dwh.CTE_xla_transaction_entities ;
create table bec_dwh.CTE_xla_transaction_entities as (
	Select ent.transaction_number TRANSACTION_NUMBER
	,ent.entity_code as ent_entity_code
	,ent.application_id as ent_application_id
	,ent.entity_id as ent_entity_id
	,ent.source_id_int_1
	From (select * from bec_ods.xla_transaction_entities where is_deleted_flg <> 'Y'
	and last_update_date >= to_date('2020-01-01 12:00:00','YYYY-MM-DD HH:MI:SS')
	) ent
);
commit;

drop table if exists bec_dwh.CTE_xla_distribution_links ;
create table bec_dwh.CTE_xla_distribution_links as (
	Select xla_dl1.alloc_to_dist_id_num_1
	,xla_dl1.ae_header_id as xla_dl1_ae_header_id
	,xla_dl1.ae_line_num as xla_dl1_ae_line_num
	,xla_dl1.source_distribution_id_num_1
	,xla_dl1.UNROUNDED_ACCOUNTED_DR ACCOUNTED_DR
	,xla_dl1.UNROUNDED_ACCOUNTED_CR ACCOUNTED_CR
	,xla_dl1.TEMP_LINE_NUM
	From bec_ods.xla_distribution_links xla_dl1 where xla_dl1.is_deleted_flg <> 'Y' 
);
commit;

drop table if exists bec_dwh.CTE_fnd_lookup_values ;
create table bec_dwh.CTE_fnd_lookup_values as (
	Select xlk2.lookup_type
	,xlk2.lookup_code
	,xlk2.meaning ACCOUNTING_CLASS_NAME
	From bec_ods.fnd_lookup_values  xlk2
	Where xlk2.lookup_type = 'XLA_ACCOUNTING_CLASS' and xlk2.is_deleted_flg <> 'Y'
); 
commit;

drop table if exists bec_dwh.CTE_fun_seq_versions1 ;
create table bec_dwh.CTE_fun_seq_versions1 
 as (

	Select 
	fsv1.header_name ACCOUNTING_SEQUENCE_NAME,
	fsv1.version_name ACCOUNTING_SEQUENCE_VERSION,
	fsv1.seq_version_id
	From bec_ods.fun_seq_versions fsv1 where is_deleted_flg <> 'Y' 
);
commit;

drop table if exists bec_dwh.CTE_OM;
create table bec_dwh.CTE_OM as (
	SELECT
		order_Number::Varchar order_Number ,
		bec.address1,
		mmt.transaction_id
	FROM
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
		(select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') oeh,
		(select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') oel,
		(select * from bec_ods.bec_customer_details_view where is_deleted_flg <> 'Y') bec
	WHERE
		1 = 1
		AND mmt.trx_source_line_id = oel.line_id
		AND oeh.header_id = oel.header_id
		AND oel.ship_to_org_id = bec.site_use_id
		LIMIT 1
);
commit;

drop table if exists bec_dwh.CTE_REC;
create table bec_dwh.CTE_REC as (
	SELECT
		hl.address1,
		rctld.cust_trx_line_gl_dist_id
	FROM
		 (select * from bec_ods.ra_cust_trx_line_gl_dist_all where is_deleted_flg <> 'Y') rctld,
		 (select * from bec_ods.ra_customer_trx_lines_all where is_deleted_flg <> 'Y') rctl,
		 (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') trx,
		 (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsu,
		 (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') hccs,
		 (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps,
		 (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl
	WHERE
		1 = 1
		--and rctld.cust_trx_line_gl_dist_id = xla_dl1.source_distribution_id_num_1
		AND rctld.customer_trx_line_id = rctl.customer_trx_line_id
		AND rctl.customer_trx_id = trx.customer_trx_id
		AND nvl(rctl.ship_to_site_use_id, trx.ship_to_site_use_id) = hcsu.site_use_id
			AND hcsu.cust_acct_site_id = hccs.cust_acct_site_id
			AND hccs.party_site_id = hps.party_site_id
			AND hps.location_id = hl.location_id
);
commit;

drop table if exists bec_dwh.CTE_ADJ;
create table bec_dwh.CTE_ADJ as (
	SELECT
		hl.address1,
		adj.adjustment_id
	FROM
		 (select * from bec_ods.ar_adjustments_all where is_deleted_flg <> 'Y') adj,
		 (select * from bec_ods.ra_customer_trx_all where is_deleted_flg <> 'Y') trx,
		 (select * from bec_ods.hz_cust_site_uses_all where is_deleted_flg <> 'Y') hcsu,
		 (select * from bec_ods.hz_cust_acct_sites_all where is_deleted_flg <> 'Y') hccs,
		 (select * from bec_ods.hz_party_sites where is_deleted_flg <> 'Y') hps,
		 (select * from bec_ods.hz_locations where is_deleted_flg <> 'Y') hl
	WHERE
		1 = 1
		--and adj.adjustment_id = ent.SOURCE_ID_INT_1
		AND adj.customer_trx_id = trx.customer_trx_id(+)
		AND trx.ship_to_site_use_id = hcsu.site_use_id(+)
		AND hcsu.cust_acct_site_id = hccs.cust_acct_site_id(+)
		AND hccs.party_site_id = hps.party_site_id(+)
		AND hps.location_id = hl.location_id(+)
);

drop table if exists bec_dwh.FACT_GL_XLA_STG;

create table bec_dwh.FACT_GL_XLA_STG
diststyle all 
sortkey(AE_HEADER_ID,AE_LINE_NUM,TEMP_LINE_NUM) as
(
With 
CTE_fun_seq_versions2 as (
	Select 
	fsv2.header_name REPORTING_SEQUENCE_NAME,
	fsv2.version_name REPORTING_SEQUENCE_VERSION,
	fsv2.seq_version_id
	From bec_ods.fun_seq_versions fsv2 
),
CTE_fnd_document_sequences as (
	Select fns.name DOCUMENT_SEQUENCE_NAME
	,fns.application_id
	,fns.doc_sequence_id
	From bec_ods.fnd_document_sequences fns where is_deleted_flg <> 'Y' 
),
CTE_gl_daily_conversion_types as (
	Select gdct.user_conversion_type CONVERSION_RATE_TYPE,
	gdct.conversion_type
	From bec_ods.gl_daily_conversion_types gdct where gdct.is_deleted_flg <> 'Y'
)
select distinct
    aeh.GL_DATE
    ,aeh.CREATION_DATE
    ,aeh.LAST_UPDATE_DATE
    ,aeh.GL_TRANSFER_DATE
    ,aeh.REFERENCE_DATE
    ,aeh.COMPLETED_DATE
    ,aeh.ACCOUNTING_SEQUENCE_NUMBER
    ,aeh.REPORTING_SEQUENCE_NUMBER
    ,aeh.DOCUMENT_SEQUENCE_NUMBER
    ,aeh.APPLICATION_ID
    ,aeh.HEADER_ID
    ,aeh.HEADER_DESCRIPTION
    ,aeh.FUND_STATUS
    ,aeh.je_category_name
    ,aeh.Event_TYPE_CODE
    ,aeh.accounting_entry_status_code
    ,aeh.gl_transfer_status_code
    ,aeh.balance_type_code
    ,aeh.budget_version_id
    ,aeh.Event_id
    ,aeh.entity_id
    ,aeh.completion_acct_seq_version_id
    ,aeh.close_acct_seq_version_id
    ,aeh.doc_sequence_id
    ,ael.gl_sl_link_table
    ,ael.gl_sl_link_id
    ,ael.encumbrance_type_id
    ,ael.accounting_class_code
    ,ael.currency_conversion_type
    ,ael.ae_header_id
    ,ael.ae_line_num
    ,ael.LINE_NUMBER
    ,ael.ORIG_LINE_NUMBER
    ,ael.LINE_DESCRIPTION
    ,ael.ENTERED_CURRENCY
    ,ael.CONVERSION_RATE
    ,ael.CONVERSION_RATE_DATE
    ,ael.CONVERSION_RATE_TYPE_CODE
    ,ael.ENTERED_DR
    ,ael.ENTERED_CR
    ,ael.UNROUNDED_ACCOUNTED_DR
    ,ael.UNROUNDED_ACCOUNTED_CR
    ,ael.STATISTICAL_AMOUNT
    ,ael.RECONCILIATION_REFERENCE
    ,ael.PARTY_TYPE_CODE
    ,xet.Event_CLASS_CODE
    ,xet.Event_TYPE_NAME
    ,xet.entity_code
    ,xect.Event_CLASS_NAME
    ,xle.TRANSACTION_DATE
    ,xle.xle_Event_ID
    ,xle.Event_DATE
    ,xle.Event_NUMBER
	,ent.TRANSACTION_NUMBER
	,ent.ent_entity_code
	,ent.ent_application_id
	,ent.ent_entity_id
	,ent.source_id_int_1
	,xla_dl1.alloc_to_dist_id_num_1
	,xla_dl1.xla_dl1_ae_header_id
	,xla_dl1.xla_dl1_ae_line_num
	,xla_dl1.source_distribution_id_num_1
	,xla_dl1.ACCOUNTED_DR
	,xla_dl1.ACCOUNTED_CR
	,xlk2.lookup_type
	,xlk2.lookup_code
	,xlk2.ACCOUNTING_CLASS_NAME  
	,fns.DOCUMENT_SEQUENCE_NAME
	,fsv1.ACCOUNTING_SEQUENCE_NAME
	,fsv1.ACCOUNTING_SEQUENCE_VERSION
	,fsv2.REPORTING_SEQUENCE_NAME
	,fsv2.REPORTING_SEQUENCE_VERSION
	,OM.order_Number
	,OM.address1
	,REC.address1 REC_address1
	,ADJ.address1 ADJ_address1
	,gdct.conversion_type
	,gdct.CONVERSION_RATE_TYPE
	,xla_dl1.TEMP_LINE_NUM
	-- audit columns
	,'N' as is_deleted_flg
	,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id
	,(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')
	|| '-' || nvl(ael.AE_HEADER_ID, 0)
	|| '-' || nvl(ael.ae_line_num, 0)	
	|| '-' || nvl(xla_dl1.TEMP_LINE_NUM, 0) as dw_load_id
	,getdate() as dw_insert_date
	,getdate() as dw_update_date
from 
bec_dwh.CTE_xla_ae_headers aeh,
bec_dwh.CTE_xla_ae_lines ael,
bec_dwh.CTE_xla_Event_types_tl xet,
bec_dwh.CTE_xla_Event_classes_tl xect,
bec_dwh.CTE_xla_Events xle,
bec_dwh.CTE_xla_transaction_entities ent,
bec_dwh.CTE_xla_distribution_links xla_dl1,
bec_dwh.CTE_fnd_lookup_values xlk2,
bec_dwh.CTE_fun_seq_versions1 fsv1,
CTE_fun_seq_versions2 fsv2,
CTE_fnd_document_sequences fns,
CTE_gl_daily_conversion_types gdct,
bec_dwh.CTE_OM OM,
bec_dwh.CTE_REC REC,
bec_dwh.CTE_ADJ ADJ
where 1=1
    AND ael.application_id = aeh.application_id
    AND ael.ae_header_id = aeh.ae_header_id 
    AND xet.application_id = aeh.application_id
    AND xet.Event_type_code = aeh.Event_type_code
    AND xect.application_id = xet.application_id
    AND xect.entity_code = xet.entity_code
    AND xect.Event_class_code = xet.Event_class_code
    AND xle.application_id = aeh.application_id
    AND xle.Event_id = aeh.Event_id
	AND ent.ent_application_id = aeh.application_id
	AND ent.ent_entity_id = aeh.entity_id
	AND xla_dl1.xla_dl1_ae_header_id = ael.ae_header_id
	AND xla_dl1.xla_dl1_ae_line_num = ael.ae_line_num
	AND xlk2.lookup_code = ael.accounting_class_code
	AND fsv1.seq_version_id(+) = aeh.completion_acct_seq_version_id
	AND fsv2.seq_version_id(+) = aeh.close_acct_seq_version_id
	AND fns.application_id(+) = aeh.application_id
	AND fns.doc_sequence_id(+) = aeh.doc_sequence_id
	AND ent.source_id_int_1 = OM.transaction_id(+)
	AND xla_dl1.source_distribution_id_num_1 = REC.cust_trx_line_gl_dist_id(+)
	AND ent.SOURCE_ID_INT_1 = adj.adjustment_id(+)
	AND gdct.conversion_type(+) = ael.currency_conversion_type	
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_gl_xla_stg'
	and batch_name = 'gl';

commit;