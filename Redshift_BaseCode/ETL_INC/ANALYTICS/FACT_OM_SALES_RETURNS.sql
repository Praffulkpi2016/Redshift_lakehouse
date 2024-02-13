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
--delete 
delete from bec_dwh.FACT_OM_SALES_RETURNS
where exists 
(
select 1
FROM BEC_ODS.OE_ORDER_LINES_ALL  OOL
inner join BEC_ODS.OE_ORDER_HEADERS_ALL   OOH on OOL.HEADER_ID         = OOH.HEADER_ID
and OOL.ORG_ID            = OOH.ORG_ID
AND OOH.cancelled_flag    = 'N'
AND OOL.cancelled_flag    = 'N'
inner join BEC_ODS.OE_TRANSACTION_TYPES_TL   OTT on 
ool.line_type_id      = ott.transaction_type_id
AND ott.NAME              = 'Return Only Line'
AND OTT.LANGUAGE          = 'US'
left outer join bec_dwh.DIM_CUSTOMER_DETAILS SHIP
on OOL.SHIP_TO_ORG_ID    =SHIP.SITE_USE_ID 
AND SHIP.SITE_USE_CODE = 'SHIP_TO'
left outer join BEC_ODS.hz_party_sites HPS
ON (HPS.PARTY_SITE_ID = SHIP.PARTY_SITE_ID) 
left outer join BEC_ODS.HZ_CUST_ACCOUNTS   HCA on OOH.SOLD_TO_ORG_ID    = HCA.CUST_ACCOUNT_ID
left outer join BEC_ODS.HZ_PARTIES   HP on HCA.PARTY_ID          = HP.PARTY_ID
inner join BEC_ODS.CS_ESTIMATE_DETAILS   CED on  OOH.HEADER_ID         = CED.ORDER_HEADER_ID
AND OOH.SOURCE_DOCUMENT_ID = CED.INCIDENT_ID
AND OOL.LINE_ID           = CED.ORDER_LINE_ID
left outer join BEC_ODS.CS_INCIDENTS_ALL_B   INC on CED.INCIDENT_ID       = INC.INCIDENT_ID
left outer join BEC_ODS.CSI_ITEM_INSTANCES   CII on INC.CUSTOMER_PRODUCT_ID = CII.INSTANCE_ID
WHERE 1 = 1 
and 
(OOH.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_sales_returns' and batch_name = 'om')
OR
OOL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_sales_returns' and batch_name = 'om')
)
and fact_om_sales_returns.dw_load_id =
    (
select
    system_id
from
    bec_etl_ctrl.etlsourceappid
where
    source_system = 'EBS'
)
 || '-' || nvl(OOH.HEADER_ID, 0)   || '-' || nvl(OOL.LINE_ID, 0)|| '-' || nvl(OOH.ORG_ID, 0)
);
commit;

insert into bec_dwh.fact_om_sales_returns
(SELECT 
OOH.ORG_ID
,OOH.ORDER_TYPE_ID
,OOH.HEADER_ID
,OOL.LINE_ID
,CED.SERIAL_NUMBER 
,CII.EXTERNAL_REFERENCE PADID
,INC.INCIDENT_NUMBER  SR_NUM
,OOL.LINE_TYPE_ID
,OOL.SHIP_FROM_ORG_ID
,OOH.ORDER_NUMBER
,OOH.ORDERED_DATE
,OOH.PRICE_LIST_ID
,OOH.CUST_PO_NUMBER
,SHIP.PARTY_NAME SHIP_TO_CUSTOMER_NAME
,HP.PARTY_NAME SOLD_TO_CUSTOMER_NAME
,SHIP.ADDRESS1
,OOL.LINE_NUMBER||'.'||OOL.SHIPMENT_NUMBER LINE_NUMBER
,OOL.ORDERED_ITEM
,OOL.INVENTORY_ITEM_ID
,NVL (OOL.unit_selling_price, 0) UNIT_SELLING_PRICE
,OOL.REQUEST_DATE
,OOL.ORDERED_QUANTITY
,OOL.FULFILLED_QUANTITY
,OOL.RETURN_REASON_CODE
,OOH.FLOW_STATUS_CODE HDR_STATUS
,OOL.FLOW_STATUS_CODE LINE_STATUS
,OOH.OPEN_FLAG HDR_OPEN_FLAG
,OOL.OPEN_FLAG LINE_OPE_FLAG
,OOL.CREATION_DATE
,OOL.LAST_UPDATE_DATE
,OOH.CREATION_DATE HDR_CREATION_DATE
,OOH.LAST_UPDATE_DATE HDR_UPDATE_DATE,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || OOH.HEADER_ID as HEADER_ID_key,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || OOL.LINE_TYPE_ID as LINE_TYPE_ID_key,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || OOH.ORG_ID as ORG_ID_key, 
ool.shipped_quantity,
HPS.party_site_name,
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
 || '-' || nvl(OOH.HEADER_ID, 0)   || '-' || nvl(OOL.LINE_ID, 0)|| '-' || nvl(OOH.ORG_ID, 0)
as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date
FROM (select * from BEC_ODS.OE_ORDER_LINES_ALL where is_deleted_flg <> 'Y') OOL
inner join (select * from BEC_ODS.OE_ORDER_HEADERS_ALL where is_deleted_flg <> 'Y')  OOH on OOL.HEADER_ID         = OOH.HEADER_ID
and OOL.ORG_ID            = OOH.ORG_ID
AND OOH.cancelled_flag    = 'N'
AND OOL.cancelled_flag    = 'N'
inner join (select * from BEC_ODS.OE_TRANSACTION_TYPES_TL where is_deleted_flg <> 'Y')  OTT on 
ool.line_type_id      = ott.transaction_type_id
AND ott.NAME              = 'Return Only Line'
AND OTT.LANGUAGE          = 'US'
left outer join bec_dwh.DIM_CUSTOMER_DETAILS SHIP
on OOL.SHIP_TO_ORG_ID    =SHIP.SITE_USE_ID 
AND SHIP.SITE_USE_CODE = 'SHIP_TO'
left outer join BEC_ODS.hz_party_sites HPS
ON (HPS.PARTY_SITE_ID = SHIP.PARTY_SITE_ID) 
left outer join (select * from BEC_ODS.HZ_CUST_ACCOUNTS where is_deleted_flg <> 'Y')  HCA on OOH.SOLD_TO_ORG_ID    = HCA.CUST_ACCOUNT_ID
left outer join (select * from BEC_ODS.HZ_PARTIES where is_deleted_flg <> 'Y')  HP on HCA.PARTY_ID          = HP.PARTY_ID
inner join (select * from BEC_ODS.CS_ESTIMATE_DETAILS where is_deleted_flg <> 'Y')  CED on  OOH.HEADER_ID         = CED.ORDER_HEADER_ID
AND OOH.SOURCE_DOCUMENT_ID = CED.INCIDENT_ID
AND OOL.LINE_ID           = CED.ORDER_LINE_ID
left outer join (select * from BEC_ODS.CS_INCIDENTS_ALL_B where is_deleted_flg <> 'Y')  INC on CED.INCIDENT_ID       = INC.INCIDENT_ID
left outer join (select * from BEC_ODS.CSI_ITEM_INSTANCES where is_deleted_flg <> 'Y')  CII on INC.CUSTOMER_PRODUCT_ID = CII.INSTANCE_ID
WHERE 1 = 1 
and 
(OOH.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_sales_returns' and batch_name = 'om')
OR
OOL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_sales_returns' and batch_name = 'om')
)
);

commit;

END;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_om_sales_returns' 
  and batch_name = 'om';
commit;