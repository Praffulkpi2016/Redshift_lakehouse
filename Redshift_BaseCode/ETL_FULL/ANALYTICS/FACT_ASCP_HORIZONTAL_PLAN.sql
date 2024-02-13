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
  table if exists bec_dwh.FACT_ASCP_HORIZONTAL_PLAN;
create table bec_dwh.fact_ascp_horizontal_plan diststyle all sortkey(
  plan_id, ORGANIZATION_ID, INVENTORY_ITEM_ID
) AS (
	SELECT 
    A.inventory_item_id,
    A.organization_id,
    A.plan_id,
    A.category_set_id,
    A.sr_instance_id,
    A.fill_kill_flag,
    A.so_line_split,
    A.quantity_rate,
    A.new_due_date,
    A.order_type_entity,
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || A.plan_id as plan_id_KEY, 
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
    )|| '-' || A.inventory_item_id as inventory_item_id_KEY ,
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
    ) || '-' || nvl(A.plan_id, 0)|| '-' || nvl(A.ORGANIZATION_ID, 0) || '-' || nvl(A.inventory_item_id, 0) || '-' || nvl(A.category_set_id, 0)
	|| '-' || nvl(A.new_due_date, '01-01-1900'::TIMESTAMP)|| '-' || nvl(A.order_type_entity, 'NA') as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  from 
    (
      (
WITH 
order_types as (
select 'Gross requirements' order_type_entity         union all
select 'Total supply'               union all
select 'Current scheduled receipts' union all
select 'Projected available balance'union all
select 'Projected on hand'          union all
select 'Sales orders'               union all
select 'Forecast'                   union all
select 'Dependent demand'           union all
select 'Expected scrap'             union all
select 'Work orders'                union all
select 'Purchase orders'             union all
select 'Requisitions/ CVMI Consumtion Plan'                union all
select 'In Transit'                 union all
select 'In Receiving'               union all
select 'Planned orders'              union all
select 'Beginning on hand'                    union all
select 'Safety stock'  )             
,Gross_Requirements AS
(SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    ABS(SUM(quantity_rate)) quantity_rate,
    trunc(mo.new_due_date) new_due_date,
    'Gross requirements' order_type_entity
FROM
    BEC_ODS.msc_orders_v mo,
	bec_dwh.DIM_ASCP_PLANS PLANS
WHERE
    ( ( mo.order_type NOT IN ( 18, 6, 7, 30, 31 ) )
      OR ( mo.order_type IN ( 18, 6, 7, 30 )
           AND mo.quantity_rate <> 0 )
      OR ( mo.order_type = 30
           AND ( mo.fill_kill_flag = 1
                 OR mo.so_line_split = 1 ) )
      OR ( mo.order_type = 31
           AND mo.quantity <> 0 ) )
    AND mo.category_set_id = 9
    --AND mo.sr_instance_id = 2021
    AND order_type <> 60
    AND action <> 'Cancel'
    AND SOURCE_TABLE = 'MSC_DEMANDS'
	and mo.plan_id = plans.plan_id
	and mo.sr_instance_id = plans.sr_instance_id
	and plans.LOAD_FLG = 'Y'
GROUP BY
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    trunc(mo.new_due_date) ,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split),
Total_Supply AS    
   ( SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    SUM(quantity_rate) quantity_rate,
    trunc(mo.new_due_date) new_due_date,
    'Total supply' order_type_entity
FROM
    BEC_ODS.msc_orders_v mo,
	bec_dwh.DIM_ASCP_PLANS plans
WHERE
    ( ( mo.order_type NOT IN ( 18, 6, 7, 30, 31 ) )
      OR ( mo.order_type IN ( 18, 6, 7, 30 )
           AND mo.quantity_rate <> 0 )
      OR ( mo.order_type = 30
           AND ( mo.fill_kill_flag = 1
                 OR mo.so_line_split = 1 ) )
      OR ( mo.order_type = 31
           AND mo.quantity <> 0 ) )
    AND mo.category_set_id = 9
    --AND mo.sr_instance_id = 2021
    AND order_type <> 60
    AND action <> 'Cancel'
    AND SOURCE_TABLE = 'MSC_SUPPLIES'
	and mo.plan_id = plans.plan_id
	and mo.sr_instance_id = plans.sr_instance_id
	and plans.LOAD_FLG = 'Y'
	/*AND order_type_text IN ('Non-standard job','Nonstandard job by-product','Work order co-product/by-product','Work order','Purchase orders',
	                         'Purchase requisition','Intransit shipment','PO in receiving','Intransit receipt','Planned order',
							 'Planned order co-product/by-product','On Hand')*/
	--AND mo.inventory_item_id  = 1338018
	--AND mo.organization_id = 265
	--AND  mo.plan_id = 40029
GROUP BY
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    trunc(mo.new_due_date),
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split),
   Current_Scheduled_Receipts AS (SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    SUM(quantity_rate) quantity_rate,
    CASE WHEN mo.OLD_DUE_DATE < plans.CURR_START_DATE THEN
    trunc(plans.CURR_START_DATE - 1)
    ELSE
    trunc(mo.OLD_DUE_DATE)
    END new_due_date,
    'Current scheduled receipts' order_type_entity
FROM
     BEC_ODS.msc_orders_v mo,
	bec_dwh.DIM_ASCP_PLANS plans
WHERE 
    ( ( mo.order_type NOT IN ( 18, 6, 7, 30, 31 ) )
      OR ( mo.order_type IN ( 18, 6, 7, 30 )
           AND mo.quantity_rate <> 0 )
      OR ( mo.order_type = 30
           AND ( mo.fill_kill_flag = 1
                 OR mo.so_line_split = 1 ) )
      OR ( mo.order_type = 31
           AND mo.quantity <> 0 ) )
    AND mo.category_set_id = 9
    --AND mo.sr_instance_id = 2021
    AND order_type <> 60
    AND (( SOURCE_TABLE = 'MSC_SUPPLIES' AND action = 'Cancel')
           OR (SOURCE_TABLE = 'MSC_SUPPLIES' AND OLD_DUE_DATE IS NOT NULL ))
    AND ORDER_TYPE_TEXT <> 'On Hand'
	and mo.plan_id = plans.plan_id
	and mo.sr_instance_id = plans.sr_instance_id
	and plans.LOAD_FLG = 'Y'
	--AND mo.inventory_item_id  = 1338018
	--AND mo.organization_id = 265
	--AND  mo.plan_id = 40029
GROUP BY
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    CASE WHEN mo.OLD_DUE_DATE < plans.CURR_START_DATE THEN
    trunc(plans.CURR_START_DATE - 1)
    ELSE
    trunc(mo.OLD_DUE_DATE)
    end ),
	prj_onhand_supply AS (SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    SUM(quantity_rate) quantity_rate,
	trunc(mo.new_due_date) new_due_date,
    'Total supply' order_type_entity
FROM
    BEC_ODS.msc_orders_v mo,
	bec_dwh.DIM_ASCP_PLANS plans
WHERE
    ( ( mo.order_type NOT IN ( 18, 6, 7, 30, 31 ) )
      OR ( mo.order_type IN ( 18, 6, 7, 30 )
           AND mo.quantity_rate <> 0 )
      OR ( mo.order_type = 30
           AND ( mo.fill_kill_flag = 1
                 OR mo.so_line_split = 1 ) )
      OR ( mo.order_type = 31
           AND mo.quantity <> 0 ) )
    AND mo.category_set_id = 9
    --AND mo.sr_instance_id = 2021
    AND order_type <> 60
    AND action <> 'Cancel'
    AND SOURCE_TABLE = 'MSC_SUPPLIES'
	AND order_type_text IN ('Non-standard job','Nonstandard job by-product','Work order co-product/by-product','Work order','On Hand')
		and mo.plan_id = plans.plan_id
	and mo.sr_instance_id = plans.sr_instance_id
	and plans.LOAD_FLG = 'Y'
	--AND mo.inventory_item_id  = 1338018
	--AND mo.organization_id = 265
	--AND  mo.plan_id = 40029
GROUP BY
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    trunc(mo.new_due_date) ,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split)
    ,prj_available_bal AS (
     SELECT 
    COALESCE(gr.inventory_item_id,ts.inventory_item_id) inventory_item_id,
    COALESCE(gr.organization_id,ts.organization_id) organization_id,
    COALESCE( gr.plan_id,ts.plan_id) plan_id,
    COALESCE(gr.category_set_id,ts.category_set_id) category_set_id,
    COALESCE( gr.sr_instance_id,ts.sr_instance_id) sr_instance_id,
    --gr.order_type,
    gr.fill_kill_flag,
    gr.so_line_split,
    NVL(ts.quantity_rate,0) - NVL(gr.quantity_rate,0) quantity_rate,
    --sum((NVL(ts.quantity_rate,0) - NVL(gr.quantity_rate,0))) OVER (ORDER BY COALESCE(gr.new_due_date,ts.new_due_date))  qty,
    COALESCE(gr.new_due_date,ts.new_due_date) new_due_date,
    'Projected available balance' order_type_entity
    FROM Gross_Requirements gr FULL OUTER JOIN Total_Supply ts
    ON  gr.inventory_item_id = ts.inventory_item_id
    AND gr.organization_id = ts.organization_id
    AND gr.plan_id = ts.plan_id
    AND gr.category_set_id = ts.category_set_id
    AND gr.sr_instance_id = ts.sr_instance_id
    AND gr.new_due_date = ts.new_due_date ) 
    ,prj_onhand_bal AS (
    SELECT 
    COALESCE(gr.inventory_item_id,ts.inventory_item_id,csr.inventory_item_id) inventory_item_id,
    COALESCE(gr.organization_id,ts.organization_id,csr.organization_id) organization_id,
    COALESCE( gr.plan_id,ts.plan_id,csr.plan_id) plan_id,
    COALESCE(gr.category_set_id,ts.category_set_id,csr.category_set_id) category_set_id,
    COALESCE( gr.sr_instance_id,ts.sr_instance_id,csr.sr_instance_id) sr_instance_id,
    --gr.order_type,
    gr.fill_kill_flag,
    gr.so_line_split,
    (NVL(csr.quantity_rate,0) + NVL(ts.quantity_rate,0) - NVL(gr.quantity_rate,0) ) quantity_rate,
    COALESCE(gr.new_due_date,ts.new_due_date,csr.new_due_date) new_due_date,
    'Projected on hand' order_type_entity
    FROM Gross_Requirements gr FULL OUTER JOIN prj_onhand_supply ts
    ON  gr.inventory_item_id = ts.inventory_item_id
    AND gr.organization_id = ts.organization_id
    AND gr.plan_id = ts.plan_id
    AND gr.category_set_id = ts.category_set_id
    AND gr.sr_instance_id = ts.sr_instance_id
    AND gr.new_due_date = ts.new_due_date 
    FULL OUTER JOIN Current_Scheduled_Receipts CSR 
    ON COALESCE(gr.inventory_item_id,ts.inventory_item_id) = CSR.inventory_item_id
    AND COALESCE(gr.organization_id,ts.organization_id) = CSR.organization_id
    AND COALESCE( gr.plan_id,ts.plan_id) = CSR.plan_id
    AND COALESCE(gr.category_set_id,ts.category_set_id) = CSR.category_set_id
    AND COALESCE( gr.sr_instance_id,ts.sr_instance_id) = CSR.sr_instance_id
    AND COALESCE(gr.new_due_date,ts.new_due_date) = CSR.new_due_date 
    )
SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    ABS(SUM(quantity_rate)) quantity_rate,
    trunc(mo.new_due_date) new_due_date,
    decode(mo.order_type_text, 'Sales Orders', 'Sales orders', 'Forecast', 'Forecast',
           'Planned order demand', 'Dependent demand', 'Work order demand', 'Dependent demand', 'Non-standard job demand',
           'Dependent demand', 'Purchase requisition scrap', 'Expected scrap', 'Purchase order scrap', 'Expected scrap',
           'Planned order scrap', 'Expected scrap', 'Intransit shipment scrap', 'Expected scrap', 'Work Order scrap',
           'Expected scrap', 'Non-standard job', 'Work orders', 'Nonstandard job by-product', 'Work orders',
           'Work order co-product/by-product', 'Work orders', 'Work order', 'Work orders', 'Purchase order',
           'Purchase orders', 'Purchase requisition', 'Requisitions/ CVMI Consumtion Plan', 'Intransit shipment', 'In Transit',
           'PO in receiving', 'In Receiving', 'Intransit receipt', 'In Receiving', 'Planned order',
           'Planned orders', 'Planned order co-product/by-product', 'Planned orders', 'On Hand', 'Beginning on hand') order_type_entity
FROM
    BEC_ODS.msc_orders_v mo,
	bec_dwh.DIM_ASCP_PLANS plans
WHERE
    ( ( mo.order_type NOT IN ( 18, 6, 7, 30, 31 ) )
      OR ( mo.order_type IN ( 18, 6, 7, 30 )
           AND mo.quantity_rate <> 0 )
      OR ( mo.order_type = 30
           AND ( mo.fill_kill_flag = 1
                 OR mo.so_line_split = 1 ) )
      OR ( mo.order_type = 31
           AND mo.quantity <> 0 ) )
    AND mo.category_set_id = 9
    --AND mo.sr_instance_id = 2021
    AND order_type <> 60
    AND action <> 'Cancel'
			and mo.plan_id = plans.plan_id
	and mo.sr_instance_id = plans.sr_instance_id
	and plans.LOAD_FLG = 'Y'
	--AND mo.inventory_item_id  = 1338018
	--AND mo.organization_id = 265
	--AND  mo.plan_id = 40029
GROUP BY
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    trunc(mo.new_due_date),
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    decode(mo.order_type_text, 'Sales Orders', 'Sales orders', 'Forecast', 'Forecast',
           'Planned order demand', 'Dependent demand', 'Work order demand', 'Dependent demand', 'Non-standard job demand',
           'Dependent demand', 'Purchase requisition scrap', 'Expected scrap', 'Purchase order scrap', 'Expected scrap',
           'Planned order scrap', 'Expected scrap', 'Intransit shipment scrap', 'Expected scrap', 'Work Order scrap',
           'Expected scrap', 'Non-standard job', 'Work orders', 'Nonstandard job by-product', 'Work orders',
           'Work order co-product/by-product', 'Work orders', 'Work order', 'Work orders', 'Purchase order',
           'Purchase orders', 'Purchase requisition', 'Requisitions/ CVMI Consumtion Plan', 'Intransit shipment', 'In Transit',
           'PO in receiving', 'In Receiving', 'Intransit receipt', 'In Receiving', 'Planned order',
           'Planned orders', 'Planned order co-product/by-product', 'Planned orders', 'On Hand', 'Beginning on hand')
UNION
select mss.INVENTORY_ITEM_ID,
mss.ORGANIZATION_ID,
mss.PLAN_ID,
mic.category_set_id,
mss.SR_INSTANCE_ID,
NULL fill_kill_flag,
NULL so_line_split,
mss.SAFETY_STOCK_QUANTITY quantity_rate,
trunc(mss.PERIOD_START_DATE) new_due_date,
'Safety stock' order_type_entity
from (SELECT * FROM BEC_ODS.MSC_SAFETY_STOCKS WHERE IS_DELETED_FLG <> 'Y') mss,
(SELECT * FROM BEC_ODS.msc_item_categories WHERE IS_DELETED_FLG <> 'Y') mic,
	bec_dwh.DIM_ASCP_PLANS plans
WHERE 
    mss.sr_instance_id = mic.sr_instance_id
    AND mss.inventory_item_id = mic.inventory_item_id
    AND mss.organization_id = mic.organization_id
    AND category_set_id = 9
	and mss.plan_id = plans.plan_id
	and mss.sr_instance_id = plans.sr_instance_id
	and plans.LOAD_FLG = 'Y'
--AND mss.plan_id = 40029 
--AND mss.INVENTORY_ITEM_ID = 1338018
--AND mss.ORGANIZATION_ID = 265
UNION
SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    quantity_rate,
    mo.new_due_date,
    order_type_entity
FROM
    Current_Scheduled_Receipts mo
UNION
  SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    quantity_rate,
    mo.new_due_date,
    order_type_entity 
FROM
    Gross_Requirements mo
   UNION
    SELECT
    mo.inventory_item_id,
    mo.organization_id,
    mo.plan_id,
    mo.category_set_id,
    mo.sr_instance_id,
    --mo.order_type,
    mo.fill_kill_flag,
    mo.so_line_split,
    quantity_rate,
    mo.new_due_date,
    'Total supply' order_type_entity
FROM
    Total_Supply mo
    UNION
    SELECT 
    pab.inventory_item_id,
    pab.organization_id,
    pab.plan_id,
    pab.category_set_id,
    pab.sr_instance_id,
    --pab.order_type,
    pab.fill_kill_flag,
    pab.so_line_split,
    (sum(quantity_rate) OVER(partition BY pab.plan_id,pab.sr_instance_id,pab.category_set_id,pab.organization_id,
	pab.inventory_item_id ORDER BY pab.new_due_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) quantity_rate,
    pab.new_due_date,
    pab.order_type_entity
    FROM prj_available_bal pab
    UNION 
    SELECT 
    pab.inventory_item_id,
    pab.organization_id,
    pab.plan_id,
    pab.category_set_id,
    pab.sr_instance_id,
    --pab.order_type,
    pab.fill_kill_flag,
    pab.so_line_split,
    (sum(quantity_rate) OVER(partition BY pab.plan_id,pab.sr_instance_id,pab.category_set_id,pab.organization_id,
	pab.inventory_item_id ORDER BY pab.new_due_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) quantity_rate,
    pab.new_due_date,
    pab.order_type_entity
    FROM prj_onhand_bal pab
    union
    SELECT
    mv.inventory_item_id,
    mv.organization_id,
    mp.plan_id,
    9 category_set_id,
    mp.sr_instance_id,
    --mo.order_type,
    null fill_kill_flag,
    null so_line_split,
    0 quantity_rate,
    trunc(mp.curr_start_date) new_due_date,  
    ot.order_type_entity
FROM
     order_types ot,
    -- bec_ods.msc_plans mp,
     bec_ods.msc_system_items  mv,
      bec_ods.msc_orders_v mov,
	bec_dwh.DIM_ASCP_PLANS mp
WHERE
 mv.plan_id = mp.plan_id
    and mv.sr_instance_id = mp.sr_instance_id
    and mov.plan_id = mv.plan_id
                and mov.sr_instance_id = mv.sr_instance_id
                and mov.inventory_item_id = mv.inventory_item_id
                and mov.organization_id = mv.organization_id
                and mov.category_set_id = 9 
    --and mv.inventory_item_id  = 1338018
    --AND mv.organization_id = 265
    --AND  mv.plan_id = 40029
	and mp.LOAD_FLG = 'Y'
group by 
mv.inventory_item_id,
    mv.organization_id,
    mp.plan_id,
    mp.sr_instance_id,
    trunc(mp.curr_start_date),
    ot.order_type_entity
	)
    ) A
);

commit;



drop table if exists bec_dwh.fact_ascp_hp_safetystock_temp;

create table bec_dwh.fact_ascp_hp_safetystock_temp as
select a.inventory_item_id,
a.organization_id,
a.plan_id,
b.category_set_id,
b.sr_instance_id,
b.fill_kill_flag,
b.so_line_split,
b.quantity_rate,
a.new_due_date,
b.new_due_date new_due_date_new from 
(select distinct plan_id,inventory_item_id,organization_id,new_due_date 
from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN) a,
(select distinct inventory_item_id,
organization_id,
plan_id,
category_set_id,
sr_instance_id,
fill_kill_flag,
so_line_split,
quantity_rate,
new_due_date
from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN 
where 1=1
and order_type_entity like 'Safe%'
and quantity_rate != 0
) b
where 1=1
and a.plan_id = b.plan_id(+)
and a.inventory_item_id = b.inventory_item_id(+)
and a.organization_id = b.organization_id(+)
and a.new_due_date =b.new_due_date(+)
order by new_due_date;


Insert into bec_dwh.FACT_ASCP_HORIZONTAL_PLAN 
(inventory_item_id,
    organization_id,
    plan_id,
    category_set_id,
    sr_instance_id,
    fill_kill_flag,
    so_line_split,
    quantity_rate,
    new_due_date,
    order_type_entity,
    plan_id_KEY, 
    ORGANIZATION_ID_KEY, 
    inventory_item_id_KEY ,
	is_deleted_flg, 
    source_app_id, 
    dw_load_id,  
    dw_insert_date, 
    dw_update_date 
)
( 
select inventory_item_id,
organization_id,
plan_id,
category_set_id_new,
sr_instance_id_new,
fill_kill_flag_new,
so_line_split_new,
quantity_rate_new,
new_due_date,
'Safety stock' order_type_entity,
(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || plan_id as plan_id_KEY, 
     (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || ORGANIZATION_ID as ORGANIZATION_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || inventory_item_id as inventory_item_id_KEY ,
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
    ) || '-' || nvl(plan_id, 0)|| '-' || nvl(ORGANIZATION_ID, 0) || '-' || nvl(inventory_item_id, 0) || '-' || nvl(category_set_id_new, 0)
	|| '-' || nvl(new_due_date, '01-01-1900'::TIMESTAMP)|| '-' || nvl(order_type_entity, 'NA') as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
	from (
SELECT
    *
    ,CASE 
        WHEN quantity_rate IS NULL THEN (
            SELECT TOP 1
                inner_table.quantity_rate
            FROM
                bec_dwh.fact_ascp_hp_safetystock_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.quantity_rate IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        quantity_rate
    END as quantity_rate_new
    ,CASE 
        WHEN category_set_id IS NULL THEN (
            SELECT TOP 1
                inner_table.category_set_id
            FROM
                bec_dwh.fact_ascp_hp_safetystock_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.category_set_id IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        category_set_id
    END as category_set_id_new
    ,CASE 
        WHEN sr_instance_id IS NULL THEN (
            SELECT TOP 1
                inner_table.sr_instance_id
            FROM
                bec_dwh.fact_ascp_hp_safetystock_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.sr_instance_id IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        sr_instance_id
    END as sr_instance_id_new
    ,CASE 
        WHEN fill_kill_flag IS NULL THEN (
            SELECT TOP 1
                inner_table.fill_kill_flag
            FROM
                bec_dwh.fact_ascp_hp_safetystock_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.fill_kill_flag IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        fill_kill_flag
    END as fill_kill_flag_new
    ,CASE 
        WHEN so_line_split IS NULL THEN (
            SELECT TOP 1
                inner_table.so_line_split
            FROM
                bec_dwh.fact_ascp_hp_safetystock_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.so_line_split IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        so_line_split
    END as so_line_split_new
FROM
    bec_dwh.fact_ascp_hp_safetystock_temp outertab
)
where new_due_date_new is null
order by new_due_date asc
);

commit;

drop table if exists bec_dwh.fact_ascp_hp_projonhand_temp;

create table bec_dwh.fact_ascp_hp_projonhand_temp as
select a.inventory_item_id,
a.organization_id,
a.plan_id,
b.category_set_id,
b.sr_instance_id,
b.fill_kill_flag,
b.so_line_split,
b.quantity_rate,
a.new_due_date,
b.new_due_date new_due_date_new from 
(select distinct plan_id,inventory_item_id,organization_id,new_due_date 
from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
where order_type_entity  = 'Safety stock') a,
(select distinct inventory_item_id,
organization_id,
plan_id,
category_set_id,
sr_instance_id,
fill_kill_flag,
so_line_split,
quantity_rate,
new_due_date
from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN 
where 1=1
and order_type_entity = 'Projected on hand'
) b
where 1=1
and a.plan_id = b.plan_id(+)
and a.inventory_item_id = b.inventory_item_id(+)
and a.organization_id = b.organization_id(+)
and a.new_due_date =b.new_due_date(+)
order by new_due_date;


Insert into bec_dwh.FACT_ASCP_HORIZONTAL_PLAN 
(inventory_item_id,
    organization_id,
    plan_id,
    category_set_id,
    sr_instance_id,
    fill_kill_flag,
    so_line_split,
    quantity_rate,
    new_due_date,
    order_type_entity,
    plan_id_KEY, 
    ORGANIZATION_ID_KEY, 
    inventory_item_id_KEY ,
	is_deleted_flg, 
    source_app_id, 
    dw_load_id,  
    dw_insert_date, 
    dw_update_date 
)
( 
select inventory_item_id,
organization_id,
plan_id,
category_set_id_new,
sr_instance_id_new,
fill_kill_flag_new,
so_line_split_new,
quantity_rate_new,
new_due_date,
'Projected on hand' order_type_entity,
(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || plan_id as plan_id_KEY, 
     (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || ORGANIZATION_ID as ORGANIZATION_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || inventory_item_id as inventory_item_id_KEY ,
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
    ) || '-' || nvl(plan_id, 0)|| '-' || nvl(ORGANIZATION_ID, 0) || '-' || nvl(inventory_item_id, 0) || '-' || nvl(category_set_id_new, 0)
	|| '-' || nvl(new_due_date, '01-01-1900'::TIMESTAMP)|| '-' || nvl(order_type_entity, 'NA') as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
	from (
SELECT
    *
    ,CASE 
        WHEN quantity_rate IS NULL THEN (
            SELECT TOP 1
                inner_table.quantity_rate
            FROM
                bec_dwh.fact_ascp_hp_projonhand_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.quantity_rate IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        quantity_rate
    END as quantity_rate_new
    ,CASE 
        WHEN category_set_id IS NULL THEN (
            SELECT TOP 1
                inner_table.category_set_id
            FROM
                bec_dwh.fact_ascp_hp_projonhand_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.category_set_id IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        category_set_id
    END as category_set_id_new
    ,CASE 
        WHEN sr_instance_id IS NULL THEN (
            SELECT TOP 1
                inner_table.sr_instance_id
            FROM
                bec_dwh.fact_ascp_hp_projonhand_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.sr_instance_id IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        sr_instance_id
    END as sr_instance_id_new
    ,CASE 
        WHEN fill_kill_flag IS NULL THEN (
            SELECT TOP 1
                inner_table.fill_kill_flag
            FROM
                bec_dwh.fact_ascp_hp_projonhand_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.fill_kill_flag IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        fill_kill_flag
    END as fill_kill_flag_new
    ,CASE 
        WHEN so_line_split IS NULL THEN (
            SELECT TOP 1
                inner_table.so_line_split
            FROM
                bec_dwh.fact_ascp_hp_projonhand_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.so_line_split IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        so_line_split
    END as so_line_split_new
FROM
    bec_dwh.fact_ascp_hp_projonhand_temp outertab
)
where new_due_date_new is null
order by new_due_date asc
);

commit;

drop table if exists bec_dwh.fact_ascp_hp_projavailbal_temp;

create table bec_dwh.fact_ascp_hp_projavailbal_temp as
select a.inventory_item_id,
a.organization_id,
a.plan_id,
b.category_set_id,
b.sr_instance_id,
b.fill_kill_flag,
b.so_line_split,
b.quantity_rate,
a.new_due_date,
b.new_due_date new_due_date_new from 
(select distinct plan_id,inventory_item_id,organization_id,new_due_date 
from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
where order_type_entity = 'Projected on hand') a,
(select distinct inventory_item_id,
organization_id,
plan_id,
category_set_id,
sr_instance_id,
fill_kill_flag,
so_line_split,
quantity_rate,
new_due_date
from bec_dwh.FACT_ASCP_HORIZONTAL_PLAN 
where 1=1
and order_type_entity = 'Projected available balance'
) b
where 1=1
and a.plan_id = b.plan_id(+)
and a.inventory_item_id = b.inventory_item_id(+)
and a.organization_id = b.organization_id(+)
and a.new_due_date =b.new_due_date(+)
order by new_due_date;


Insert into bec_dwh.FACT_ASCP_HORIZONTAL_PLAN 
(inventory_item_id,
    organization_id,
    plan_id,
    category_set_id,
    sr_instance_id,
    fill_kill_flag,
    so_line_split,
    quantity_rate,
    new_due_date,
    order_type_entity,
    plan_id_KEY, 
    ORGANIZATION_ID_KEY, 
    inventory_item_id_KEY ,
	is_deleted_flg, 
    source_app_id, 
    dw_load_id,  
    dw_insert_date, 
    dw_update_date 
)
( 
select inventory_item_id,
organization_id,
plan_id,
category_set_id_new,
sr_instance_id_new,
fill_kill_flag_new,
so_line_split_new,
quantity_rate_new,
new_due_date,
'Projected available balance' order_type_entity,
(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || plan_id as plan_id_KEY, 
     (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || ORGANIZATION_ID as ORGANIZATION_ID_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || inventory_item_id as inventory_item_id_KEY ,
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
    ) || '-' || nvl(plan_id, 0)|| '-' || nvl(ORGANIZATION_ID, 0) || '-' || nvl(inventory_item_id, 0) || '-' || nvl(category_set_id_new, 0)
	|| '-' || nvl(new_due_date, '01-01-1900'::TIMESTAMP)|| '-' || nvl(order_type_entity, 'NA') as dw_load_id,  
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
	from (
SELECT
    *
    ,CASE 
        WHEN quantity_rate IS NULL THEN (
            SELECT TOP 1
                inner_table.quantity_rate
            FROM
                bec_dwh.fact_ascp_hp_projavailbal_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.quantity_rate IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        quantity_rate
    END as quantity_rate_new
    ,CASE 
        WHEN category_set_id IS NULL THEN (
            SELECT TOP 1
                inner_table.category_set_id
            FROM
                bec_dwh.fact_ascp_hp_projavailbal_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.category_set_id IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        category_set_id
    END as category_set_id_new
    ,CASE 
        WHEN sr_instance_id IS NULL THEN (
            SELECT TOP 1
                inner_table.sr_instance_id
            FROM
                bec_dwh.fact_ascp_hp_projavailbal_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.sr_instance_id IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        sr_instance_id
    END as sr_instance_id_new
    ,CASE 
        WHEN fill_kill_flag IS NULL THEN (
            SELECT TOP 1
                inner_table.fill_kill_flag
            FROM
                bec_dwh.fact_ascp_hp_projavailbal_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.fill_kill_flag IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        fill_kill_flag
    END as fill_kill_flag_new
    ,CASE 
        WHEN so_line_split IS NULL THEN (
            SELECT TOP 1
                inner_table.so_line_split
            FROM
                bec_dwh.fact_ascp_hp_projavailbal_temp as inner_table
            WHERE 1=1
                AND inner_table.plan_id = outertab.plan_id
				AND inner_table.inventory_item_id = outertab.inventory_item_id
				AND inner_table.organization_id = outertab.organization_id
				
                AND inner_table.new_due_date < outertab.new_due_date
                AND inner_table.so_line_split IS NOT NULL
            ORDER BY
                inner_table.new_due_date DESC
        )
    ELSE
        so_line_split
    END as so_line_split_new
FROM
    bec_dwh.fact_ascp_hp_projavailbal_temp outertab
)
where new_due_date_new is null
order by new_due_date asc
);

commit;

end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_ascp_horizontal_plan' 
  and batch_name = 'ascp';
commit;