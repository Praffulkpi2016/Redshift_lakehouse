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

delete from bec_dwh.FACT_OM_TRANS_TYPES
where (nvl(TRANSACTION_TYPE_ID, 0)
,nvl(ASSIGNED_LINE_TYPE_ID, 0)
,nvl(ITEM_TYPE,'NA')
,nvl(START_DATE_ACTIVE ,'1900-01-01 12:00:00')
,nvl(END_DATE_ACTIVE,'1900-01-01 12:00:00')
)
in
(
select ods.TRANSACTION_TYPE_ID,
ods.ASSIGNED_LINE_TYPE_ID,
ods.ITEM_TYPE,
ods.START_DATE_ACTIVE,
ods.END_DATE_ACTIVE
from bec_dwh.fact_om_trans_types dw,
(
SELECT 
nvl(otta.TRANSACTION_TYPE_ID, 0)   as TRANSACTION_TYPE_ID
,nvl(OWA_L.LINE_TYPE_ID, 0) as ASSIGNED_LINE_TYPE_ID
,nvl(OWA_L.ITEM_TYPE_CODE,'NA') as ITEM_TYPE
,nvl(OWA_L.START_DATE_ACTIVE ,'1900-01-01 12:00:00') as START_DATE_ACTIVE
,nvl(OWA_L.END_DATE_ACTIVE,'1900-01-01 12:00:00') as END_DATE_ACTIVE
FROM bec_ods.OE_TRANSACTION_TYPES_ALL OTTA
     ,bec_ods.OE_WORKFLOW_ASSIGNMENTS  OWA_H
     ,bec_ods.OE_WORKFLOW_ASSIGNMENTS  OWA_L
	 ,bec_ods.RA_BATCH_SOURCES_ALL  RABSA
where 1 = 1
--AND OTTA.TRANSACTION_TYPE_CODE = 'ORDER'
AND OTTA.TRANSACTION_TYPE_ID    = OWA_H.order_type_id(+) 
--AND OTTA.TRANSACTION_TYPE_ID = 1005
AND (OWA_H.END_DATE_ACTIVE IS NULL OR OWA_H.END_DATE_ACTIVE >= SYSDATE)
AND OWA_H.LINE_TYPE_ID IS NULL
AND OWA_H.order_type_id        = OWA_L.order_type_id(+)
AND NVL(OWA_L.LINE_TYPE_ID(+), -1) > 0
AND OTTA.INVOICE_SOURCE_ID     = RABSA.BATCH_SOURCE_ID(+)
AND OTTA.ORG_ID 			   = RABSA.ORG_ID(+)
AND NVL(OWA_L.LINE_TYPE_ID(+), -1) > 0
and ( 
					OTTA.kca_seq_date > (
									select
										(executebegints-prune_days)
									from
										bec_etl_ctrl.batch_dw_info
									where
										dw_table_name = 'fact_om_trans_types'
										and batch_name = 'om')
										 
										or
					OWA_L.kca_seq_date > (
									select
										(executebegints-prune_days)
									from
										bec_etl_ctrl.batch_dw_info
									where
										dw_table_name = 'fact_om_trans_types'
										and batch_name = 'om')
					OR OTTA.is_deleted_flg = 'Y'
					OR OWA_L.is_deleted_flg = 'Y'
					OR OWA_H.is_deleted_flg = 'Y'
					OR RABSA.is_deleted_flg = 'Y'
 
)
)ods
where 1=1
and dw.dw_load_id =
	    (
    select
        system_id
    from
        bec_etl_ctrl.etlsourceappid
    where
        source_system = 'EBS'
    )
	 || '-' || nvl(ODS.TRANSACTION_TYPE_ID, 0)   
	 || '-' || nvl(ODS.ASSIGNED_LINE_TYPE_ID, 0)
	 || '-' || nvl(ODS.ITEM_TYPE,'NA')
	 || '-' || nvl(ODS.START_DATE_ACTIVE ,'1900-01-01 12:00:00')
	 || '-' || nvl(ODS.END_DATE_ACTIVE,'1900-01-01 12:00:00')
);

commit;

insert into bec_dwh.fact_om_trans_types
(
org_id
,transaction_type_id
,order_category_code
,order_process_name
,process_start_date
,hdr_item_type
,price_list_id
,warehouse_id
,shipping_method_code
,shipment_priority_code
,freight_terms_code
,invoice_source_id
,INVOICE_SOURCE
,invoicing_rule_id
,accounting_rule_id
,cust_trx_type_id
,cost_of_goods_sold_account
,conversion_type_code
,currency_code
,assigned_line_type_id
,line_item_type
,item_type
,line_process_name
,start_date_active
,end_date_active
,creation_date
,created_by
,last_update_date
,last_updated_by
,assignment_id
,org_id_key
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
SELECT otta.org_id
      ,otta.TRANSACTION_TYPE_ID
      ,otta.ORDER_CATEGORY_CODE --Lookups dimension lookup type: ORDER_CATEGORY
      ,OWA_H.PROCESS_NAME ORDER_PROCESS_NAME --wf_activities.
	  ,OTTA.START_DATE_ACTIVE  PROCESS_START_DATE
      ,OWA_H.WF_ITEM_TYPE   HDR_ITEM_TYPE   
	  ,OTTA.PRICE_LIST_ID
	  ,OTTA.WAREHOUSE_ID
	  ,OTTA.SHIPPING_METHOD_CODE  --SHIPPING_METHOD
	  ,OTTA.SHIPMENT_PRIORITY_CODE  --SHIPMENT_PRIORITY
	  ,OTTA.FREIGHT_TERMS_CODE   --FREIGHT TERMS
	  ,OTTA.INVOICE_SOURCE_ID  --RA_BATCH_SOURCES_ALL
	  ,RABSA.NAME INVOICE_SOURCE
	  ,OTTA.INVOICING_RULE_ID  --dim_ar_invoice_rules
	  ,OTTA.ACCOUNTING_RULE_ID --dim_ar_invoice_rules
	  ,OTTA.CUST_TRX_TYPE_ID   --dim_ar_cust_trx_type
	  ,OTTA.COST_OF_GOODS_SOLD_ACCOUNT
	  ,OTTA.CONVERSION_TYPE_CODE
	  ,OTTA.CURRENCY_CODE	  
      ,OWA_L.LINE_TYPE_ID ASSIGNED_LINE_TYPE_ID
      ,'OEOL'  LINE_ITEM_TYPE
      ,OWA_L.ITEM_TYPE_CODE ITEM_TYPE
	  ,OWA_L.PROCESS_NAME LINE_PROCESS_NAME
      ,OWA_L.START_DATE_ACTIVE 
      ,OWA_L.END_DATE_ACTIVE      
      ,OTTA.CREATION_DATE
	  ,OTTA.CREATED_BY
	  ,OTTA.LAST_UPDATE_DATE
	  ,OTTA.LAST_UPDATED_BY
	  ,OWA_H.ASSIGNMENT_ID,
	  (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || otta.org_id as ORG_ID_key,
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
	 || '-' || nvl(otta.TRANSACTION_TYPE_ID, 0)   
	 || '-' || nvl(OWA_L.LINE_TYPE_ID, 0)
	 || '-' || nvl(OWA_L.ITEM_TYPE_CODE,'NA')
	 || '-' || nvl(OWA_L.START_DATE_ACTIVE ,'1900-01-01 12:00:00')
	 || '-' || nvl(OWA_L.END_DATE_ACTIVE,'1900-01-01 12:00:00')
	as dw_load_id,
    getdate() as dw_insert_date,
    getdate() as dw_update_date 
FROM  (select * from bec_ods.OE_TRANSACTION_TYPES_ALL where is_deleted_flg <> 'Y')OTTA
     ,(select * from bec_ods.OE_WORKFLOW_ASSIGNMENTS where is_deleted_flg <> 'Y') OWA_H
     ,(select * from bec_ods.OE_WORKFLOW_ASSIGNMENTS where is_deleted_flg <> 'Y') OWA_L
	 ,(select * from bec_ods.RA_BATCH_SOURCES_ALL where is_deleted_flg <> 'Y') RABSA
where 1 = 1
--AND OTTA.TRANSACTION_TYPE_CODE = 'ORDER'
AND OTTA.TRANSACTION_TYPE_ID    = OWA_H.order_type_id(+) 
--AND OTTA.TRANSACTION_TYPE_ID = 1005
AND (OWA_H.END_DATE_ACTIVE IS NULL OR OWA_H.END_DATE_ACTIVE >= SYSDATE)
AND OWA_H.LINE_TYPE_ID IS NULL
AND OWA_H.order_type_id        = OWA_L.order_type_id(+)
AND NVL(OWA_L.LINE_TYPE_ID(+), -1) > 0
AND OTTA.INVOICE_SOURCE_ID     = RABSA.BATCH_SOURCE_ID(+)
AND OTTA.ORG_ID 			   = RABSA.ORG_ID(+)
	   and ( 
					OTTA.kca_seq_date > (
									select
										(executebegints-prune_days)
									from
										bec_etl_ctrl.batch_dw_info
									where
										dw_table_name = 'fact_om_trans_types'
										and batch_name = 'om')
										 
										or
					OWA_L.kca_seq_date > (
									select
										(executebegints-prune_days)
									from
										bec_etl_ctrl.batch_dw_info
									where
										dw_table_name = 'fact_om_trans_types'
										and batch_name = 'om')
 
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
  dw_table_name = 'fact_om_trans_types' 
  and batch_name = 'om';
commit;

