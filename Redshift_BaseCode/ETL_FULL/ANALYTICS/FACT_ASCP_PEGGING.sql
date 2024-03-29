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
drop 
  table if exists bec_dwh.FACT_ASCP_PEGGING;
create table bec_dwh.FACT_ASCP_PEGGING diststyle even interleaved sortkey(
 ORGANIZATION_CODE,COMPILE_DESIGNATOR,ORDER_NUMBER,PLANNER_CODE,
PLANNING_MAKE_BUY_CODE) AS 
 (
 SELECT 
    A.DISP_ITEM_SEGMENTS,
    A.PLAN_ID,
    A.COMPILE_DESIGNATOR,
    A.ORGANIZATION_CODE,
    A.ORGANIZATION_ID,
	A.SOURCE_ORGANIZATION_ID,
    A.ITEM_SEGMENTS,
    A.REQUESTED_START_DATE,
    A.REQUESTED_COMPLETION_DATE,
    A.ORDER_TYPE_TEXT,
    A.NEW_DUE_DATE,
    A.SHIP_SET_NAME,
    A.ARRIVAL_SET_NAME,
    A.SCHEDULE_ARRIVAL_DATE,
    A.SCHEDULE_SHIP_DATE,
    A.ACTUAL_START_DATE,
    A.PLANNED_ARRIVAL_DATE,
    A.LATEST_ACCEPTABLE_DATE,
    A.QUANTITY_RATE,
    A.QTY_BY_DUE_DATE,
    A.PO_LINE_ID,
    A.SHIPMENT_ID,
    A.RESCHEDULE_DAYS,
    A.OLD_DUE_DATE,
    A.SR_INSTANCE_ID,
    A.DISPOSITION_ID,
    A.DESCRIPTION,
    A.ORIGINAL_ITEM_NAME,
    A.PREV_SUBST_ITEM,
    A.PREV_SUBST_ORG,
    A.ORIGINAL_ITEM_QTY,
    A.LAST_UNIT_COMPLETION_DATE,
    A.LINE_CODE,
    A.EXPIRATION_DATE,
    A.ORDER_NUMBER,
    A.ACTION,
    A.MRP_PLANNING_CODE_TEXT,
    A.OLD_ORDER_QUANTITY,
    A.ORIG_ORG_CODE,
    A.DEST_ORG_CODE,
    A.DEST_ORG_ID,
    A.DEST_INST_ID,
    A.SHIP_METHOD,
    A.ORIG_SHIP_METHOD,
    A.SOURCE_VENDOR_NAME,
    A.SOURCE_VENDOR_SITE_CODE,
    A.PLANNING_GROUP,
    A.SCHEDULE_GROUP_NAME,
    A.QUANTITY,
    A.SHIP_DATE,
    A.ASSEMBLY_DEMAND_COMP_DATE,
    A.PLANNER_CODE,
    A.VENDOR_ID,
    A.VENDOR_SITE_ID,
    A.CATEGORY_ID,
    A.SOURCE_VENDOR_SITE_ID,
    A.SOURCE_VENDOR_ID,
    A.SUPPLIER_NAME,
    A.SUPPLIER_SITE_CODE,
    A.SUBINVENTORY_CODE,
    A.MRP_PLANNING_CODE,
    A.SOURCE_ORGANIZATION_CODE,
    A.STATUS_CODE,
    A.CUSTOMER_ID,
    A.CUSTOMER_NAME,
    A.SHIP_TO_SITE_ID,
    A.SHIP_TO_SITE_NAME,
    A.CUSTOMER_SITE_ID,
    A.CUSTOMER_SITE_NAME,
    A.PROMISE_DATE,
    A.PROMISE_SHIP_DATE,
    A.ORIGINAL_NEED_BY_DATE,
    A.REQUEST_DATE,
    A.REQUEST_SHIP_DATE,
    A.CATEGORY_SET_ID,
    A.SALES_ORDER_LINE_ID,
    A.SHIP_SET_ID,
    A.CUSTOMER_PO_NUMBER,
    A.CUSTOMER_PO_LINE_NUMBER,
    A.BUYER_NAME,
    A.SOURCE_TABLE,
    A.TRANSACTION_ID,
    A.INVENTORY_ITEM_ID,
    A.LEVEL_SEQ,
    A.PEGGING_ID,
    A.PREV_PEGGING_ID,
    A.PARENT_PEGGING_ID,
    A.DEMAND_ID,
    A.TRANSACTION_ID CHILD_TRANS_ID,
    A.MSC_TRXN_ID,
    A.DEMAND_QTY,
    A.DEMAND_DATE,
    A.ITEM_ID,
    A.ITEM_ORG,
    A.PEGGED_QTY,
    A.ORIGINATION_NAME,
    A.END_PEGGED_QTY,
    A.END_DEMAND_QTY,
    A.END_DEMAND_DATE,
    A.END_ITEM_ORG,
    A.END_ORIGINATION_NAME,
    A.END_SATISFIED_DATE,
    A.END_DISPOSITION,
    A.ORDER_TYPE,
    A.END_DEMAND_CLASS,
    A.OP_SEQ_NUM,
    A.ITEM_DESC_ORG,
    /*SUBSTR(MSC_GET_NAME.ITEM_DESC(A.INVENTORY_ITEM_ID, B.ORGANIZATION_ID, B.PLAN_ID, B.SR_INSTANCE_ID),1,20)                                              ITEM_DESC, */
    --A.PREV_ITEM_NAME,
    A.PREV_ITEM_ORG,
    A.PREV_ITEM_DESC,
    --A.PREV_SUPPLY_DATE,
    --A.PREV_SUPPLY_QTY,
    A.PREV_ITEM_ID,
    --A.PREV_DEMAND_ID,
    A.PREV_PEGGED_QTY,
	A.PREV_ORDER_TYPE,
    /*XXBEC_ASCP_REPORT_PKG.XXBEC_PREV_ORDER_TYPE (B.PEGGING_ID) PREV_ORDER_TYPE,*/
    A.SUPPLY_DATE, 
    --A.DISPOSITION,
    A.SUPPLY_QTY,
    A.NEW_ORDER_DATE,
    A.NEW_DOCK_DATE,
    A.NEW_START_DATE,
    A.VMI_FLAG,                                 
    A.CVMI_FLAG,                                
    A.PREPROCESSING_LEAD_TIME,
    A.PROCESSING_LEAD_TIME,
    A.POSTPROCESSING_LEAD_TIME,
    A.PLANNING_MAKE_BUY_CODE,
    A.MATERIAL_COST,
    A.CAT_SEGMENT1,
    A.CAT_SEGMENT2,
    A.ITEM_CATEGORY,
    A.INVENTORY_ITEM_STATUS_CODE,
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.PLAN_ID as PLAN_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.SR_INSTANCE_ID as SR_INSTANCE_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.SHIPMENT_ID as SHIPMENT_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.CUSTOMER_ID as CUSTOMER_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.CATEGORY_ID as CATEGORY_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.ORGANIZATION_ID as ORGANIZATION_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.vendor_id as vendor_id_KEY ,
	(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.PEGGING_ID as PEGGING_ID_KEY,-- audit columns
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
	|| '-' || nvl(A.PEGGING_ID, 0) 
	|| '-' || nvl(A.PARENT_PEGGING_ID, 0) 
	|| '-' || nvl(A.ITEM_ID, 0) 
	|| '-' || nvl(A.transaction_id, 0)
	|| '-' || nvl(A.level_seq, 0)
	|| '-' || NVL(A.supply_date, '1900-01-01 12:00:00') 
	|| '-' || NVL(A.end_demand_date, '1900-01-01 12:00:00') 
	|| '-' || nvl(A.supply_qty, 0)
	|| '-' || NVL(A.demand_date, '1900-01-01 12:00:00') 
	|| '-' || nvl(A.pegged_qty, 0)
	|| '-' || nvl(A.cvmi_flag, 'NA')
	|| '-' || nvl(A.vmi_flag, 'NA') as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  from 
    (
      (
    WITH 
 SUP AS (
    SELECT DISTINCT
        PASL.OWNING_ORGANIZATION_ID,
        APS.VENDOR_NAME,
        PASL.VENDOR_ID,
        MSI.SEGMENT1                               SEGMENT1,
        NVL(PAA.ENABLE_VMI_FLAG, 'N')              VMI_FLAG,
        NVL(PAA.CONSIGNED_FROM_SUPPLIER_FLAG, 'N') CVMI_FLAG
    FROM
        (SELECT * FROM BEC_ODS.PO_APPROVED_SUPPLIER_LIST WHERE is_deleted_flg <> 'Y') PASL ,
        (SELECT * FROM BEC_ODS.PO_ASL_ATTRIBUTES  WHERE is_deleted_flg <> 'Y')        PAA,
        (SELECT * FROM BEC_ODS.AP_SUPPLIERS WHERE is_deleted_flg <> 'Y')             APS,
        (SELECT * FROM BEC_ODS.MTL_SYSTEM_ITEMS_B   WHERE is_deleted_flg <> 'Y')       MSI
    WHERE
            PASL.VENDOR_ID = APS.VENDOR_ID
        AND PASL.ASL_ID = PAA.ASL_ID
        AND PASL.OWNING_ORGANIZATION_ID = MSI.ORGANIZATION_ID
        AND PASL.ITEM_ID = MSI.INVENTORY_ITEM_ID
        AND PASL.ASL_STATUS_ID = 2
        AND NVL(PASL.DISABLE_FLAG, 'N') = 'N'
), 
 MIC AS (
    SELECT
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        MAX(CATEGORY_ID) CATEGORY_ID
    FROM
        BEC_ODS.MTL_ITEM_CATEGORIES 
    WHERE
        CATEGORY_SET_ID = 1
		AND is_deleted_flg <> 'Y'
    GROUP BY
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID
), 
MSC_FLP_DEMAND_SUPPLY_V3 AS (
    SELECT
        MFP2.PEGGING_ID                  PEGGING_ID,
        ROUND(MR2.NEW_ORDER_QUANTITY, 6) SUPPLY_QTY,
        MR2.NEW_SCHEDULE_DATE            SUPPLY_DATE
    /*MSC_GET_NAME.SUPPLY_ORDER_NUMBER(MR2.ORDER_TYPE, MR2.ORDER_NUMBER, MR2.PLAN_ID, MR2.SR_INSTANCE_ID, MR2.TRANSACTION_ID,
                                     MR2.DISPOSITION_ID) DISPOSITION*/
    FROM
        (SELECT * FROM BEC_ODS.MSC_SYSTEM_ITEMS WHERE is_deleted_flg <> 'Y') MIF2,
        (SELECT * FROM BEC_ODS.MSC_SUPPLIES WHERE is_deleted_flg <> 'Y')    MR2,
        (SELECT * FROM BEC_ODS.MSC_FULL_PEGGING WHERE is_deleted_flg <> 'Y') MFP2,
        (SELECT * FROM BEC_ODS.MSC_PLANS   WHERE is_deleted_flg <> 'Y')     MP
    WHERE
            MR2.TRANSACTION_ID = MFP2.TRANSACTION_ID
        AND MR2.PLAN_ID = MFP2.PLAN_ID
        AND MIF2.INVENTORY_ITEM_ID = MFP2.INVENTORY_ITEM_ID
        AND MIF2.SR_INSTANCE_ID = MFP2.SR_INSTANCE_ID
        AND MIF2.ORGANIZATION_ID = MFP2.ORGANIZATION_ID
        AND MIF2.PLAN_ID = MFP2.PLAN_ID
        AND MP.PLAN_ID = MFP2.PLAN_ID
), 
PREVIOUS_PEGGING AS (
    SELECT
        pegging_id,
        prev_pegging_id,
        prev_item_id,
        prev_item_org,
        prev_item_desc,
        prev_pegged_qty,
        prev_order_type
    FROM
       BEC_ODS.BEC_PEGGING_DATA_V 
    group by 
        pegging_id,
        prev_pegging_id,
        prev_item_id,
        prev_item_org,
        prev_item_desc,
        prev_pegged_qty,
        prev_order_type
)
SELECT
    LPAD(B.ITEM_ORG, (LENGTH(B.ITEM_ORG) + B.LEVEL_SEQ)::int, '*') DISP_ITEM_SEGMENTS,
    A.PLAN_ID,
    A.COMPILE_DESIGNATOR,
    A.ORGANIZATION_CODE,
    A.ORGANIZATION_ID,
	A.SOURCE_ORGANIZATION_ID,
    A.ITEM_SEGMENTS,
    A.REQUESTED_START_DATE,
    A.REQUESTED_COMPLETION_DATE,
    A.ORDER_TYPE_TEXT,
    A.NEW_DUE_DATE,
    A.SHIP_SET_NAME,
    A.ARRIVAL_SET_NAME,
    A.SCHEDULE_ARRIVAL_DATE,
    A.SCHEDULE_SHIP_DATE,
    A.ACTUAL_START_DATE,
    A.PLANNED_ARRIVAL_DATE,
    A.LATEST_ACCEPTABLE_DATE,
    A.QUANTITY_RATE,
    A.QTY_BY_DUE_DATE,
    A.PO_LINE_ID,
    A.SHIPMENT_ID,
    A.RESCHEDULE_DAYS,
    A.OLD_DUE_DATE,
    A.SR_INSTANCE_ID,
    A.DISPOSITION_ID,
    A.DESCRIPTION,
    A.ORIGINAL_ITEM_NAME,
    A.PREV_SUBST_ITEM,
    A.PREV_SUBST_ORG,
    A.ORIGINAL_ITEM_QTY,
    A.LAST_UNIT_COMPLETION_DATE,
    A.LINE_CODE,
    A.EXPIRATION_DATE,
    A.ORDER_NUMBER,
    A.ACTION,
    A.MRP_PLANNING_CODE_TEXT,
    A.OLD_ORDER_QUANTITY,
    A.ORIG_ORG_CODE,
    A.DEST_ORG_CODE,
    A.DEST_ORG_ID,
    A.DEST_INST_ID,
    A.SHIP_METHOD,
    A.ORIG_SHIP_METHOD,
    A.SOURCE_VENDOR_NAME,
    A.SOURCE_VENDOR_SITE_CODE,
    A.PLANNING_GROUP,
    A.SCHEDULE_GROUP_NAME,
    A.QUANTITY,
    A.SHIP_DATE,
    A.ASSEMBLY_DEMAND_COMP_DATE,
    A.PLANNER_CODE,
    A.VENDOR_ID,
    A.VENDOR_SITE_ID,
    A.CATEGORY_ID,
    A.SOURCE_VENDOR_SITE_ID,
    A.SOURCE_VENDOR_ID,
    A.SUPPLIER_NAME,
    A.SUPPLIER_SITE_CODE,
    A.SUBINVENTORY_CODE,
    A.MRP_PLANNING_CODE,
    SOR.ORGANIZATION_CODE SOURCE_ORGANIZATION_CODE,
    A.STATUS_CODE,
    A.CUSTOMER_ID,
    A.CUSTOMER_NAME,
    A.SHIP_TO_SITE_ID,
    A.SHIP_TO_SITE_NAME,
    A.CUSTOMER_SITE_ID,
    A.CUSTOMER_SITE_NAME,
    A.PROMISE_DATE,
    A.PROMISE_SHIP_DATE,
    A.ORIGINAL_NEED_BY_DATE,
    A.REQUEST_DATE,
    A.REQUEST_SHIP_DATE,
    A.CATEGORY_SET_ID,
    A.SALES_ORDER_LINE_ID,
    A.SHIP_SET_ID,
    A.CUSTOMER_PO_NUMBER,
    A.CUSTOMER_PO_LINE_NUMBER,
    A.BUYER_NAME,
    A.SOURCE_TABLE,
    A.TRANSACTION_ID,
    A.INVENTORY_ITEM_ID,
    B.LEVEL_SEQ,
    B.PEGGING_ID,
    B.PREV_PEGGING_ID,
    B.PARENT_PEGGING_ID,
    B.DEMAND_ID,
    B.TRANSACTION_ID CHILD_TRANS_ID,
    B.MSC_TRXN_ID,
    B.DEMAND_QTY,
    B.DEMAND_DATE,
    B.ITEM_ID,
    B.ITEM_ORG,
    B.PEGGED_QTY,
    B.ORIGINATION_NAME,
    B.END_PEGGED_QTY,
    B.END_DEMAND_QTY,
    B.END_DEMAND_DATE,
    B.END_ITEM_ORG,
    B.END_ORIGINATION_NAME,
    B.END_SATISFIED_DATE,
    B.END_DISPOSITION,
    B.ORDER_TYPE,
    B.END_DEMAND_CLASS,
    B.OP_SEQ_NUM,
    B.ITEM_DESC_ORG,
   /*SUBSTR(MSC_GET_NAME.ITEM_DESC(A.INVENTORY_ITEM_ID, B.ORGANIZATION_ID, B.PLAN_ID, B.SR_INSTANCE_ID),1,20)                                              ITEM_DESC, */
    --PP.ITEM_NAME                                            PREV_ITEM_NAME,
    PP.prev_item_org                                             prev_item_org,
    PP.PREV_ITEM_DESC                                     PREV_ITEM_DESC,
    --PP.SUPPLY_DATE                                          PREV_SUPPLY_DATE,
    --PP.SUPPLY_QUANTITY                                      PREV_SUPPLY_QTY,
    PP.PREV_ITEM_ID                                    PREV_ITEM_ID,
    --PP.DEMAND_ID                                            PREV_DEMAND_ID,
    PP.prev_pegged_qty                                   PREV_PEGGED_QTY,
	pp.PREV_ORDER_TYPE,
    /*XXBEC_ASCP_REPORT_PKG.XXBEC_PREV_ORDER_TYPE (B.PEGGING_ID) PREV_ORDER_TYPE,*/
    C.SUPPLY_DATE, 
    --C.DISPOSITION,
    C.SUPPLY_QTY,
    B.NEW_ORDER_DATE,
    B.NEW_DOCK_DATE,
    B.NEW_START_DATE,
    NVL(SUP.VMI_FLAG, 'N')                                  VMI_FLAG,
    NVL(SUP.CVMI_FLAG, 'N')                                 CVMI_FLAG,
    MSI.PREPROCESSING_LEAD_TIME,
    MSI.FULL_LEAD_TIME                                      PROCESSING_LEAD_TIME,
    MSI.POSTPROCESSING_LEAD_TIME,
    DECODE(MSI.PLANNING_MAKE_BUY_CODE, 1, 'MAKE', 2, 'BUY') PLANNING_MAKE_BUY_CODE,
    CIC.MATERIAL_COST,
    MC.SEGMENT1                                             CAT_SEGMENT1,
    MC.SEGMENT2                                             CAT_SEGMENT2,
    MC.SEGMENT1 || '.' || MC.SEGMENT2  ITEM_CATEGORY,
    MSI.INVENTORY_ITEM_STATUS_CODE
FROM
    (SELECT * FROM BEC_ODS.XXBEC_MSC_ORDERS WHERE  is_deleted_flg <> 'Y')  A,
    (SELECT * FROM BEC_ODS.XXBEC_PEGGING_DATA WHERE  is_deleted_flg <> 'Y') B,
    (SELECT * FROM BEC_ODS.MTL_SYSTEM_ITEMS_B WHERE  is_deleted_flg <> 'Y')         MSI,
    (SELECT * FROM BEC_ODS.MTL_CATEGORIES_B WHERE  is_deleted_flg <> 'Y')           MC,
    (SELECT * FROM BEC_ODS.CST_ITEM_COSTS WHERE is_deleted_flg <> 'Y' )          CIC,
    SUP                      SUP,
    MIC                      MIC,
    MSC_FLP_DEMAND_SUPPLY_V3 C,
    (SELECT * FROM BEC_ODS.ORG_ORGANIZATION_DEFINITIONS WHERE  is_deleted_flg <> 'Y') SOR,
    PREVIOUS_PEGGING         PP
WHERE 1=1
    AND A.TRANSACTION_ID = B.MSC_TRXN_ID (+)
    AND B.PEGGING_ID = C.PEGGING_ID (+)
    AND A.ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND A.ITEM_SEGMENTS = MSI.SEGMENT1
    AND MSI.ORGANIZATION_ID = MIC.ORGANIZATION_ID (+)
    AND MSI.INVENTORY_ITEM_ID = MIC.INVENTORY_ITEM_ID (+)
    AND MIC.CATEGORY_ID = MC.CATEGORY_ID (+)
    AND MSI.ORGANIZATION_ID = CIC.ORGANIZATION_ID
    AND MSI.INVENTORY_ITEM_ID = CIC.INVENTORY_ITEM_ID
    AND CIC.COST_TYPE_ID = 1
    AND A.SOURCE_VENDOR_NAME = SUP.VENDOR_NAME (+)
    AND A.ORGANIZATION_ID = SUP.OWNING_ORGANIZATION_ID (+)
    AND A.ITEM_SEGMENTS = SUP.SEGMENT1 (+)
    AND B.PREV_PEGGING_ID = PP.PREV_PEGGING_ID (+)
    and B.PEGGING_ID = PP.PEGGING_ID (+)
    AND A.SOURCE_ORGANIZATION_ID = SOR.ORGANIZATION_ID(+)
    --AND item_segments = '130676'   and demand_date = to_date('2023-03-31','YYYY-MM-DD')
    --AND  A.PLAN_ID = 40029
	)
    ) A
);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_ascp_pegging' 
  and batch_name = 'ascp';
commit;
