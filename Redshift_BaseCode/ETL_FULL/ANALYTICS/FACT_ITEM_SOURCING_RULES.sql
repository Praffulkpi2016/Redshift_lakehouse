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

drop table if exists bec_dwh.FACT_ITEM_SOURCING_RULES;

create table bec_dwh.FACT_ITEM_SOURCING_RULES diststyle all sortkey(assignment_id)
as 
(
select
    sso.SR_SOURCE_ID
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sso.SR_SOURCE_ID as SR_SOURCE_ID_KEY
    ,sr_asgn.assignment_id
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sr_asgn.assignment_id as assignment_id_key
    ,sr_asgn.assignment_set_id
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sr_asgn.assignment_set_id as assignment_set_id_key
	,sr_asgn.sourcing_rule_id
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sr_asgn.sourcing_rule_id as sourcing_rule_id_key
	,sr.sourcing_rule_name
	,sr.description sourcing_rule_desc
    ,sro.effective_date
	,sro.disable_date
    ,sso.vendor_id 
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sso.vendor_id as vendor_id_key
	,sso.vendor_site_id 
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sso.vendor_site_id as  vendor_site_id_key
    ,sso.allocation_percent
	,sso.RANK 
	,sso.ship_method
    ,sm.intransit_time
    ,DECODE(sso.source_type,0,'ATP',1, 'Transfer From', 2, 'Make At',3, 'Buy From', 4, 'Return To', 5, 'Repair At') source_type
    ,sr.organization_id organization_id
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sr.organization_id as  organization_id_key
	,sro.RECEIPT_ORGANIZATION_ID
    ,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sro.RECEIPT_ORGANIZATION_ID as	RECEIPT_ORGANIZATION_ID_KEY
	--sro.organization_code mrp_organization,
    ,sr.planning_active
	,sr_asgn.customer_id
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sr_asgn.customer_id as customer_id_key
    ,sr_asgn.ship_to_site_id
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sr_asgn.ship_to_site_id as ship_to_site_id_key
	,msib.segment1 item
    ,msib.description item_desc 
	,item_cat.item_category
    ,item_cat.category_set_name 
    ,mas.assignment_set_name
    ,sso.SOURCE_ORGANIZATION_ID
	,(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||sso.SOURCE_ORGANIZATION_ID as SOURCE_ORGANIZATION_ID_KEY
	--, sso.SOURCE_ORGANIZATION_CODE
    ,sr_asgn.inventory_item_id
	--,sr_asgn.organization_id
	,null customer_name
	,null ship_to_address
	,sr_asgn.sourcing_rule_type
 -- audit columns

   ,'N' as is_deleted_flg,
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
    || '-' || nvl(sr_asgn.assignment_id, 0)
	|| '-' || nvl(sr_asgn.assignment_set_id, 0)
	|| '-' || nvl(sr_asgn.sourcing_rule_id, 0)	
	|| '-' || nvl(sso.SR_SOURCE_ID, 0)
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM (select * from bec_ods.mrp_sr_source_org where is_deleted_flg<>'Y') sso,
     (select * from bec_ods.mrp_sr_receipt_org where is_deleted_flg<>'Y') sro,
     (select * from bec_ods.mrp_sourcing_rules where is_deleted_flg<>'Y') sr,
     --mtl_system_items_b msi,
     (SELECT mic.inventory_item_id,mic.organization_id, mc.segment2 item_category
             ,CSET.CATEGORY_SET_NAME 
 FROM (select * from bec_ods.mtl_item_categories where is_deleted_flg<>'Y') mic,
		     (select * from bec_ods.mtl_categories_b where is_deleted_flg<>'Y') mc,
		     (select * from bec_ods.MTL_CATEGORY_SETS_TL where is_deleted_flg<>'Y') CSET,
			 (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msi
       WHERE 1=1
        AND MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID
        AND MIC.CATEGORY_ID = MC.CATEGORY_ID
        AND CSET.LANGUAGE ='US'
        AND CSET.CATEGORY_SET_NAME = 'Inventory' 
		AND mic.inventory_item_id = msi.inventory_item_id
		AND mic.organization_id = msi.organization_id
		AND msi.enabled_flag = 'Y'
        AND msi.inventory_item_status_code = 'Active') item_cat,
     (select * from bec_ods.mrp_assignment_sets where is_deleted_flg<>'Y') mas,
     (select * from bec_ods.mrp_sr_assignments 
	  where is_deleted_flg<>'Y'
	  AND assignment_type IN (1, 2, 4, 5)
                            OR organization_id IS NOT NULL
                            OR (    assignment_type IN (3, 6)
                                AND organization_id IS NULL
                                AND ((inventory_item_id IN (
                                                    SELECT inventory_item_id
                                                      FROM bec_ods.mtl_system_items_b
                                                     WHERE organization_id = 90)
                                     )
                                    )
                               )) sr_asgn,
     (select * from bec_ods.MTL_INTERORG_SHIP_METHODS where is_deleted_flg<>'Y') SM,
	 (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y' AND organization_id = 106) msib
WHERE 1 = 1
AND sro.sr_receipt_id = sso.sr_receipt_id
AND sr.sourcing_rule_id = sro.sourcing_rule_id
AND NVL (sr.organization_id, 106) = NVL (sro.receipt_organization_id, 106)
AND sr_asgn.assignment_set_id = mas.assignment_set_id(+)
AND sr_asgn.sourcing_rule_id(+) = sro.sourcing_rule_id
--AND sr_asgn.inventory_item_id = msi.inventory_item_id(+)
-- AND NVL (sr_asgn.organization_id, 106) = NVL (msi.organization_id(+),-106)
AND mas.assignment_set_name(+) = 'BLOOM ASSIGNMENT SET'
AND NVL (sr_asgn.organization_id(+), 106) = NVL (sro.receipt_organization_id, 106)
AND sr_asgn.inventory_item_id = item_cat.inventory_item_id(+)
AND sr_asgn.organization_id = item_cat.organization_id(+)
AND sr_asgn.inventory_item_id = msib.inventory_item_id(+)
AND NVL (sr_asgn.organization_id, 106) = msib.organization_id(+)
AND sso.ship_method = sm.ship_method(+)
AND ((sso.SHIP_METHOD IS NULL)
      OR (SM.FROM_ORGANIZATION_ID = sso.SOURCE_ORGANIZATION_ID
      AND SM.TO_ORGANIZATION_ID = sro.RECEIPT_ORGANIZATION_ID)
 )
ORDER BY sr.sourcing_rule_name
  
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_item_sourcing_rules'
	and batch_name = 'inv';

commit;