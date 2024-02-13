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
drop  table if exists bec_dwh.FACT_OM_TRANS_TYPES;
create table bec_dwh.FACT_OM_TRANS_TYPES diststyle all sortkey(TRANSACTION_TYPE_ID ) as 
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
);
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
