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
delete from bec_dwh.FACT_OM_HOLDS
where exists 
(
select 1
from 
bec_ods.OE_ORDER_HOLDS_ALL OHL
inner join bec_ods.OE_ORDER_HEADERS_ALL OOHL
on OHL.HEADER_ID   = OOHL.HEADER_ID
left outer join bec_ods.OE_ORDER_LINES_ALL	 	 OOLA
on OHL.LINE_ID     = OOLA.LINE_ID
and OOHL.HEADER_ID  = OOLA.HEADER_ID
and OOHL.ORG_ID     = OOLA.ORG_ID
left outer join bec_ods.OE_TRANSACTION_TYPES_TL    OTTT_H
on OOHL.ORDER_TYPE_ID = OTTT_H.TRANSACTION_TYPE_ID
and OTTT_H.LANGUAGE = 'US'
left outer join bec_dwh.DIM_CUSTOMER_DETAILS   BCDV
on OOHL.SHIP_TO_ORG_ID = BCDV.SITE_USE_ID
and OOHL.SOLD_TO_ORG_ID = BCDV.CUST_ACCOUNT_ID
left outer join 
bec_ods.OE_HOLD_SOURCES_ALL 	 OHS
on OHL.HOLD_SOURCE_ID  = OHS.HOLD_SOURCE_ID
left outer join bec_ods.OE_HOLD_RELEASES	 OHR
on OHL.HOLD_RELEASE_ID = OHR.HOLD_RELEASE_ID
left outer join bec_ods.OE_HOLD_DEFINITIONS 	 OHD
on OHS.HOLD_ID         = OHD.HOLD_ID
left outer join bec_ods.OE_HOLD_AUTHORIZATIONS     OHA 
on OHD.HOLD_ID         = OHA.HOLD_ID
and OHA.AUTHORIZED_ACTION_CODE = 'REMOVE'
left outer join bec_ods.FND_RESPONSIBILITY_TL      FRTL
on OHA.RESPONSIBILITY_ID  = FRTL.RESPONSIBILITY_ID
AND FRTL.LANGUAGE  = 'US'
left outer join bec_ods.FND_LOOKUP_VALUES  	 HT
on OHD.TYPE_CODE  = HT.LOOKUP_CODE
and HT.LOOKUP_TYPE = 'HOLD_TYPE'
AND HT.language = 'US' 
where 1=1
and 
(OOHL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_holds' and batch_name = 'om')
OR
OHL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_holds' and batch_name = 'om')
)
and FACT_OM_HOLDS.dw_load_id =
(
select
    system_id
from
    bec_etl_ctrl.etlsourceappid
where
    source_system = 'EBS'
)
 || '-' || nvl(OOHL.HEADER_ID, 0)   || '-' || nvl(OHL.ORDER_HOLD_ID, 0)
)
;
commit;

insert into  bec_dwh.FACT_OM_HOLDS
(
Select 
HEADER_ID
,LINE_TYPE_ID
,ORDER_HOLD_ID
,HOLD_CREATED_BY
,RELEASE_CREATED_BY
--,RESPONSIBILITY_ID
,ORG_ID
,CUSTOMER
,CUSTOMER_NUMBER
,ORDER_NUMBER
,LINE_NUMBER
,ORDER_TYPE
,DATE_ORDERED
,ORDERED_ITEM
,QTY
,HOLD_NAME
,HOLD_DESCRIPTION
,HOLD_AT
,HOLD_UNTIL
,HOLD_APPLIED_DATE
,RELEASED_DATE
,RELEASED_REASON
,RELEASE_COMMENT
,RESPONSIBILITY_NAME
,HOLD_TYPE
,HEADER_ORDER_STATUS
,LINE_ORDER_STATUS
,WF_ITEM
,WF_ACTIVITY,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || HEADER_ID as HEADER_ID_key,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || LINE_TYPE_ID as LINE_TYPE_ID_key,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || ORDER_HOLD_ID as ORDER_HOLD_ID_key,
--(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || RESPONSIBILITY_ID as RESPONSIBILITY_ID_key,
(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || ORG_ID as ORG_ID_key, 
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
 || '-' || nvl(HEADER_ID, 0)   || '-' || nvl(ORDER_HOLD_ID, 0) --|| '-' || nvl(responsibility_id, 0)
as dw_load_id,
getdate() as dw_insert_date,
getdate() as dw_update_date 
  from 
(
SELECT   
 OOHL.HEADER_ID
,OOLA.LINE_TYPE_ID
,OHL.ORDER_HOLD_ID
,OHL.CREATED_BY HOLD_CREATED_BY
,OHR.CREATED_BY RELEASE_CREATED_BY
--,OHA.RESPONSIBILITY_ID
,OHL.ORG_ID
,BCDV.PARTY_NAME        CUSTOMER
,BCDV.ACCOUNT_NUMBER    CUSTOMER_NUMBER
,OOHL.ORDER_NUMBER      ORDER_NUMBER
,DECODE(OHL.LINE_ID,NULL,NULL,OOLA.LINE_NUMBER||'.'||OOLA.SHIPMENT_NUMBER)   LINE_NUMBER
,OTTT_H.NAME            ORDER_TYPE
,OOHL.ORDERED_DATE     DATE_ORDERED
,OOLA.ORDERED_ITEM
,OOLA.ORDERED_QUANTITY   QTY
,OHD.NAME HOLD_NAME
,OHD.DESCRIPTION HOLD_DESCRIPTION
,DECODE(OHL.LINE_ID,NULL,'Order', 'Line') AS HOLD_AT
,OHS.HOLD_UNTIL_DATE HOLD_UNTIL
,OHL.CREATION_DATE HOLD_APPLIED_DATE
,OHR.CREATION_DATE RELEASED_DATE
,OHR.RELEASE_REASON_CODE  RELEASED_REASON
,OHR.RELEASE_COMMENT
--,FRTL.RESPONSIBILITY_NAME  
,HT.MEANING  HOLD_TYPE 
,OOHL.FLOW_STATUS_CODE HEADER_ORDER_STATUS
,OOLA.FLOW_STATUS_CODE LINE_ORDER_STATUS
,OHD.ITEM_TYPE  WF_ITEM
,OHD.ACTIVITY_NAME  WF_ACTIVITY
,LISTAGG(FRTL.RESPONSIBILITY_NAME, '; ') WITHIN GROUP (ORDER BY OOHL.HEADER_ID) as RESPONSIBILITY_NAME
from 
(select * from bec_ods.OE_ORDER_HOLDS_ALL where is_deleted_flg <> 'Y') OHL
inner join (select * from bec_ods.OE_ORDER_HEADERS_ALL  where is_deleted_flg <> 'Y') OOHL
on OHL.HEADER_ID   = OOHL.HEADER_ID
left outer join (select * from bec_ods.OE_ORDER_LINES_ALL where is_deleted_flg <> 'Y') 	 	 OOLA
on OHL.LINE_ID     = OOLA.LINE_ID
and OOHL.HEADER_ID  = OOLA.HEADER_ID
and OOHL.ORG_ID     = OOLA.ORG_ID
left outer join (select * from bec_ods.OE_TRANSACTION_TYPES_TL  where is_deleted_flg <> 'Y')    OTTT_H
on OOHL.ORDER_TYPE_ID = OTTT_H.TRANSACTION_TYPE_ID
and OTTT_H.LANGUAGE = 'US'
left outer join bec_dwh.DIM_CUSTOMER_DETAILS   BCDV
on OOHL.SHIP_TO_ORG_ID = BCDV.SITE_USE_ID
and OOHL.SOLD_TO_ORG_ID = BCDV.CUST_ACCOUNT_ID
left outer join 
(select * from bec_ods.OE_HOLD_SOURCES_ALL where is_deleted_flg <> 'Y')			 OHS
on OHL.HOLD_SOURCE_ID  = OHS.HOLD_SOURCE_ID
left outer join (select * from bec_ods.OE_HOLD_RELEASES	 where is_deleted_flg <> 'Y')		 OHR
on OHL.HOLD_RELEASE_ID = OHR.HOLD_RELEASE_ID
left outer join (select * from bec_ods.OE_HOLD_DEFINITIONS where is_deleted_flg <> 'Y')		 OHD
on OHS.HOLD_ID         = OHD.HOLD_ID
left outer join (select * from bec_ods.OE_HOLD_AUTHORIZATIONS  where is_deleted_flg <> 'Y')     OHA 
on OHD.HOLD_ID         = OHA.HOLD_ID
and OHA.AUTHORIZED_ACTION_CODE = 'REMOVE'
left outer join (select * from bec_ods.FND_RESPONSIBILITY_TL  where is_deleted_flg <> 'Y')      FRTL
on OHA.RESPONSIBILITY_ID  = FRTL.RESPONSIBILITY_ID
AND FRTL.LANGUAGE  = 'US'
left outer join (select * from bec_ods.FND_LOOKUP_VALUES  where is_deleted_flg <> 'Y')			 HT
on OHD.TYPE_CODE  = HT.LOOKUP_CODE
and HT.LOOKUP_TYPE = 'HOLD_TYPE'
AND HT.language = 'US'
where 1=1
and 
(OOHL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_holds' and batch_name = 'om')
OR
OHL.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_om_holds' and batch_name = 'om')
)
GROUP BY OOHL.HEADER_ID
,OOLA.LINE_TYPE_ID
,OHL.ORDER_HOLD_ID
,OHL.CREATED_BY 
,OHR.CREATED_BY 
--,OHA.RESPONSIBILITY_ID
,OHL.ORG_ID
,BCDV.PARTY_NAME        
,BCDV.ACCOUNT_NUMBER    
,OOHL.ORDER_NUMBER      
,DECODE(OHL.LINE_ID,NULL,NULL,OOLA.LINE_NUMBER||'.'||OOLA.SHIPMENT_NUMBER)   
,OTTT_H.NAME            
,OOHL.ORDERED_DATE     
,OOLA.ORDERED_ITEM
,OOLA.ORDERED_QUANTITY   
,OHD.NAME 
,OHD.DESCRIPTION 
,DECODE(OHL.LINE_ID,NULL,'Order', 'Line')  
,OHS.HOLD_UNTIL_DATE 
,OHL.CREATION_DATE 
,OHR.CREATION_DATE 
,OHR.RELEASE_REASON_CODE  
,OHR.RELEASE_COMMENT
--,FRTL.RESPONSIBILITY_NAME  
,HT.MEANING   
,OOHL.FLOW_STATUS_CODE 
,OOLA.FLOW_STATUS_CODE 
,OHD.ITEM_TYPE  
,OHD.ACTIVITY_NAME
    )
);
END;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_om_holds' 
  and batch_name = 'om';
commit;
