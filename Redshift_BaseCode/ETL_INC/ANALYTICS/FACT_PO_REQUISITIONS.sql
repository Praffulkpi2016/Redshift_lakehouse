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

Truncate table bec_dwh.FACT_PO_REQUISITIONS_TMP;

Insert Into bec_dwh.FACT_PO_REQUISITIONS_TMP
(
        SELECT DISTINCT
              PD.PO_LINE_ID,
		PD.PO_HEADER_ID,
		RD.REQUISITION_LINE_ID,
		POH.AGENT_ID AS BUYER_ID,
		POH.TYPE_LOOKUP_CODE AS PO_TYPE,
		NVL(POLL.consigned_flag,'N') as CVMI_FLAG,
		MAX(poll.po_release_id) as po_release_id
        FROM
            (select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y')        PD,
            (select * from bec_ods.PO_REQ_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y')    RD,
			(select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg <> 'Y')    POLL,
			(select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg <> 'Y')    POH
        WHERE
            PD.REQ_DISTRIBUTION_ID (+) = RD.DISTRIBUTION_ID
			AND PD.PO_HEADER_ID = POLL.PO_HEADER_ID(+)
			AND PD.PO_LINE_ID   = POLL.PO_LINE_ID(+)
			AND PD.LINE_LOCATION_ID = POLL.LINE_LOCATION_ID(+)
			AND POLL.PO_HEADER_ID = POH.PO_HEADER_ID(+)
			GROUP BY PD.PO_LINE_ID,
		PD.PO_HEADER_ID,
		RD.REQUISITION_LINE_ID,
		POH.AGENT_ID ,
		POH.TYPE_LOOKUP_CODE ,
		NVL(POLL.consigned_flag,'N')
    );
    
Truncate table bec_dwh.FACT_PO_REQUISITIONS_TMP_SBQ;

Insert Into bec_dwh.FACT_PO_REQUISITIONS_TMP_SBQ
  (
    select
		EMPLOYEE_ID
		,OBJECT_ID,OBJECT_TYPE_CODE,SEQUENCE_NUM
from
		(select * from bec_ods.PO_ACTION_HISTORY where is_deleted_flg <> 'Y')  PAH1,
		(select * from bec_ods.PO_REQUISITION_HEADERS_ALL where is_deleted_flg <> 'Y')  RQ
where
		PAH1.OBJECT_ID = RQ.REQUISITION_HEADER_ID
	and PAH1.OBJECT_TYPE_CODE = 'REQUISITION'
	and PAH1.SEQUENCE_NUM = (
	select
		MAX(SEQUENCE_NUM)
	from
		(select * from bec_ods.PO_ACTION_HISTORY where is_deleted_flg <> 'Y')  PAH1,
		(select * from bec_ods.PO_REQUISITION_HEADERS_ALL where is_deleted_flg <> 'Y')  RQ
	where
		PAH1.OBJECT_ID = RQ.REQUISITION_HEADER_ID
		and PAH1.OBJECT_TYPE_CODE = 'REQUISITION'
            )
    );
    
Truncate table bec_dwh.FACT_PO_REQUISITIONS_TMP_aprvr;
Insert Into bec_dwh.FACT_PO_REQUISITIONS_TMP_aprvr  
 (
select employee_id , object_id  
from (select * from bec_ods.PO_ACTION_HISTORY where is_deleted_flg <> 'Y')
where (object_id,sequence_num) in 
(select object_id,sequence_num from 
(SELECT MAX(SEQUENCE_NUM) as sequence_num ,object_id 
FROM (select * from bec_ods.PO_ACTION_HISTORY where is_deleted_flg <> 'Y')
WHERE 1=1
AND object_type_code = 'REQUISITION'
group by object_id
)
)
);

Truncate table bec_dwh.FACT_PO_REQUISITIONS_TMP_aprvr_new;
Insert Into bec_dwh.FACT_PO_REQUISITIONS_TMP_aprvr_new 
 (
select employee_id , object_id , action_date
from bec_ods.PO_ACTION_HISTORY 
where (object_id,sequence_num) in 
(select object_id,sequence_num from 
(SELECT object_id,
case when MAX(SEQUENCE_NUM)in (0,1) then MAX(SEQUENCE_NUM)
else MAX(SEQUENCE_NUM)-1 end as sequence_num 
FROM bec_ods.PO_ACTION_HISTORY 
WHERE 1=1
and is_deleted_flg <> 'Y'
AND object_type_code = 'REQUISITION'
group by object_id
)
)
);


Truncate table bec_dwh.FACT_PO_REQUISITIONS;

Insert Into bec_dwh.FACT_PO_REQUISITIONS
(
SELECT distinct
    RQ.ORG_ID,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RQ.ORG_ID  ORG_ID_KEY,
    RQ.REQUISITION_HEADER_ID,
    RL.REQUISITION_LINE_ID,
    RD.DISTRIBUTION_ID         REQ_DISTRIBUTION_ID,
    RL.VENDOR_ID,
    RL.VENDOR_SITE_ID,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RL.VENDOR_ID  VENDOR_ID_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||RL.VENDOR_SITE_ID  VENDOR_SITE_ID_KEY, 
    RQ.PREPARER_ID,
    RQ.SEGMENT1,
    RL.LINE_NUM,
    RQ.CREATION_DATE           RH_CREATION_DATE,
    RQ.AUTHORIZATION_STATUS    REQUISITION_STATUS,
    RQ.CLOSED_CODE             RH_CLOSED_CODE,
    RL.UNIT_PRICE,
    RL.QUANTITY,
    NVL(RL.AMOUNT, RL.UNIT_PRICE * RL.QUANTITY) RL_AMOUNT,
    RL.CURRENCY_CODE,
    RL.ITEM_ID,	
  (select system_id from bec_etl_ctrl.etlsourceappid   where source_system='EBS')||'-'||RL.ITEM_ID  ITEM_ID_KEY,
    RL.TO_PERSON_ID            REQUESTOR,
    RQ.DESCRIPTION             REQUISITION_DESCRIPTION,
    RL.ITEM_DESCRIPTION        REQUISITION_LINE_DESCRIPTION,
    RD.CODE_COMBINATION_ID,
    RL.CATEGORY_ID,
    PRL.PO_HEADER_ID ,
	PRL.PO_LINE_ID,
    --RQ.APPROVED_DATE,
	aprvr_new.action_date APPROVED_DATE,
	SBQ.EMPLOYEE_ID NEXT_APPROVER_ID,
    RD.PROJECT_ID,
    RD.TASK_ID,
    RL.TO_PERSON_ID            DELIVER_TO,
    RL.NEED_BY_DATE,
    RL.SUGGESTED_BUYER_ID,
    RL.RATE_DATE               EXCHANGE_DATE,
    RL.RATE                    EXCHANGE_RATE,
    RL.RATE_TYPE               EXCHANGE_RATE_TYPE,
    RQ.INTERFACE_SOURCE_CODE   IMPORT_SOURCE,
    RQ.TYPE_LOOKUP_CODE        REQUISITION_TYPE,
    RQ.CANCEL_FLAG             REQUISITION_HEADER_CANCEL,
    RL.CANCEL_FLAG             REQ_LINE_CANCEL,
    RQ.CREATED_BY,
    RQ.START_DATE_ACTIVE,
    RQ.END_DATE_ACTIVE,
    RL.LINE_TYPE_ID,
    RL.LINE_LOCATION_ID,
    RL.SOURCE_TYPE_CODE,
    RL.CLOSED_DATE,
    RL.CLOSED_CODE,
    RL.destination_organization_id INVENTORY_ORGANIZATION_ID,
    RL.SUGGESTED_VENDOR_NAME,
    RL.DESTINATION_TYPE_CODE,
    RL.DELIVER_TO_LOCATION_ID,
    RL.QUANTITY_DELIVERED,
    RL.VENDOR_CONTACT_ID,
    RL.CLOSED_REASON,
    RL.QUANTITY_RECEIVED,
    RL.BASE_UNIT_PRICE,
	RD.last_update_date,
	aprvr.employee_id as next_approver,
	aprvr.employee_id as last_approver,
	aprvr_new.employee_id as next_approver_new,
	aprvr_new.employee_id as last_approver_new,
	PRL.BUYER_ID,
	PRL.CVMI_FLAG,
	PRL.PO_TYPE,
	POR.release_date as po_date,
	RL.justification,
    RL.urgent_flag,
    RL.unit_meas_lookup_code,
	'N' AS IS_DELETED_FLG,
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
     || '-'|| nvl(RD.DISTRIBUTION_ID,0)
     || '-'|| nvl(aprvr.employee_id, 0)   
     || '-'|| nvl(aprvr_new.employee_id, 0)
	 || '-'|| nvl(RQ.AUTHORIZATION_STATUS,'NA')  as dw_load_id,
    getdate() as dw_insert_date,
    getdate() as dw_update_date     
FROM
   (select * from bec_ods.PO_REQUISITION_HEADERS_ALL where is_deleted_flg <> 'Y')      RQ,
    (select * from bec_ods.PO_REQUISITION_LINES_ALL where is_deleted_flg <> 'Y')        RL,
    (select * from bec_ods.PO_REQ_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y')        RD,
    bec_dwh.FACT_PO_REQUISITIONS_TMP PRL,
	bec_dwh.FACT_PO_REQUISITIONS_TMP_SBQ SBQ,
	bec_dwh.FACT_PO_REQUISITIONS_TMP_aprvr aprvr,
	bec_dwh.FACT_PO_REQUISITIONS_TMP_aprvr_new aprvr_new
,(select release_date,po_release_id,po_header_id from bec_ods.PO_RELEASES_ALL 
where is_deleted_flg <> 'Y')    POR
WHERE
    1 = 1
    AND RQ.REQUISITION_HEADER_ID = RL.REQUISITION_HEADER_ID
    AND RL.REQUISITION_LINE_ID   = RD.REQUISITION_LINE_ID
    AND RL.REQUISITION_LINE_ID = PRL.REQUISITION_LINE_ID (+) 
    and RQ.REQUISITION_HEADER_ID = SBQ.OBJECT_ID(+)
	AND RQ.REQUISITION_HEADER_ID = aprvr.object_id(+)
	AND RQ.REQUISITION_HEADER_ID = aprvr_new.object_id(+)
	AND PRL.po_release_id = POR.po_release_id(+)
    AND PRL.po_header_id = POR.po_header_id(+)
	);
    END;
	
 update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_po_requisitions'
	and batch_name = 'po';

commit;