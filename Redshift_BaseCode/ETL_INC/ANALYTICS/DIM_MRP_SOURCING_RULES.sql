/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;     

-- Delete Records

delete
from
	bec_dwh.DIM_MRP_SOURCING_RULES
where
	(nvl(sourcing_rule_id, 0),
	nvl(assignment_id, 0),
	nvl(SR_RECEIPT_ID,0),
	nvl(sr_source_id,0)) in (
	select
		nvl(ods.sourcing_rule_id, 0) as sourcing_rule_id,
	     nvl(ods.assignment_id, 0) as assignment_id,
	     nvl(ods.SR_RECEIPT_ID,0) as SR_RECEIPT_ID,
	     nvl(ods.sr_source_id,0) as sr_source_id
	from
		bec_dwh.DIM_MRP_SOURCING_RULES dw,
		(select
            msra.sourcing_rule_id,
        	msra.assignment_id,
            s.sr_receipt_id,
            src.sr_source_id
FROM
    bec_ods.mrp_sourcing_rules msr,
		   
    bec_ods.mrp_sr_assignments msra,
    bec_ods.mrp_sr_receipt_org s,
    bec_ods.mrp_sr_source_org  src,
	bec_ods.mtl_parameters mp
WHERE
    msr.sourcing_rule_id = msra.sourcing_rule_id
    and msra.assignment_set_id  = 1  
    and msra.sourcing_rule_id = s.sourcing_rule_id(+) 
    and s.sr_receipt_id   = src.sr_receipt_id(+) 
	AND src.source_organization_id = mp.organization_id(+)
	 	and (msra.kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_mrp_sourcing_rules'
				and batch_name = 'po'))
        	)  ods 
     where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.sourcing_rule_id, 0) 
			|| '-' || nvl(ods.assignment_id,0)
			|| '-' || nvl(ods.SR_RECEIPT_ID,0) || '-' || nvl(ods.sr_source_id,0) 
			);
 
commit;
-- Insert records

insert
	into
	bec_dwh.DIM_MRP_SOURCING_RULES
(
   assignment_id,
    assignment_set_id,
    assignment_type,
    organization_id,
    customer_id,
    ship_to_site_id,
    sourcing_rule_type,
    sourcing_rule_id,
    sourcing_rule_name,
    status,
    planning_active,
    category_id,
    category_set_id,
    inventory_item_id,
    secondary_inventory,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    sr_receipt_id,
    s_sourcing_rule_id,
    receipt_organization_id,
    effective_date,
    disable_date,
    sr_source_id,
    src_sr_receipt_id,
    source_organization_id,
    vendor_id,
    vendor_site_id,
    source_type,
    allocation_percent,
    rank,
    ship_method,
	source_organization_code,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)

(
select
	assignment_id,
    assignment_set_id,
    assignment_type,
    organization_id,
    customer_id,
    ship_to_site_id,
    sourcing_rule_type,
    sourcing_rule_id,
    sourcing_rule_name,
    status,
    planning_active,
    category_id,
    category_set_id,
    inventory_item_id,
    secondary_inventory,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    sr_receipt_id,
    s_sourcing_rule_id,
    receipt_organization_id,
    effective_date,
    disable_date,
    sr_source_id,
    src_sr_receipt_id,
    source_organization_id,
    vendor_id,
    vendor_site_id,
    source_type,
    allocation_percent,
    rank,
    ship_method,
	source_organization_code,
	--Audit COLUMNS
	
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
    || '-'
       || nvl(sourcing_rule_id, 0) || '-' || nvl(assignment_id,0) || '-' || nvl(SR_RECEIPT_ID,0) || '-' || nvl(sr_source_id,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
(select
	msra.assignment_id,
    msra.assignment_set_id,
    msra.assignment_type,
    msra.organization_id,
    msra.customer_id,
    msra.ship_to_site_id,
    msra.sourcing_rule_type,
    msra.sourcing_rule_id  ,
    msr.sourcing_rule_name,
    msr.status,
    msr.planning_active,
    msra.category_id,
    msra.category_set_id,
    msra.inventory_item_id,
    msra.secondary_inventory,
    msra.last_update_date,
    msra.last_updated_by,
    msra.creation_date,
    msra.created_by,
    msra.last_update_login,
    s.sr_receipt_id,
    s.sourcing_rule_id as s_sourcing_rule_id,
    s.receipt_organization_id,
    s.effective_date,
    s.disable_date,
    src.sr_source_id,
    src.sr_receipt_id as src_sr_receipt_id,
    src.source_organization_id,
    src.vendor_id,
    src.vendor_site_id,
    src.source_type,
    src.allocation_percent,
    src.rank,
    src.ship_method,
	mp.organization_code as source_organization_code
FROM
    bec_ods.mrp_sourcing_rules msr, 
    bec_ods.mrp_sr_assignments msra,
    bec_ods.mrp_sr_receipt_org s,
    bec_ods.mrp_sr_source_org  src,
	bec_ods.mtl_parameters mp
WHERE
    msr.sourcing_rule_id = msra.sourcing_rule_id
    and msra.assignment_set_id  = 1 
    and msra.sourcing_rule_id = s.sourcing_rule_id(+) 
    and s.sr_receipt_id   = src.sr_receipt_id(+) 
	AND src.source_organization_id = mp.organization_id(+)
 and
		(
	msra.kca_seq_date > (
		select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_mrp_sourcing_rules'
				and batch_name = 'po') 			
				 )
)
 );
 commit;
-- Soft delete


update
	bec_dwh.DIM_MRP_SOURCING_RULES 
	set
	is_deleted_flg = 'Y'
where
	(nvl(sourcing_rule_id, 0),
	nvl(assignment_id, 0),
	nvl(SR_RECEIPT_ID,0),
	nvl(sr_source_id,0)) not in (
	select
		nvl(ods.sourcing_rule_id, 0) as sourcing_rule_id,
	     nvl(ods.assignment_id, 0) as assignment_id,
	     nvl(ods.SR_RECEIPT_ID,0) as SR_RECEIPT_ID,
	     nvl(ods.sr_source_id,0) as sr_source_id
	from
		bec_dwh.DIM_MRP_SOURCING_RULES dw,
		(select
            msra.sourcing_rule_id,
        	msra.assignment_id,
            s.sr_receipt_id,
            src.sr_source_id
FROM
    bec_ods.mrp_sourcing_rules msr,
		   
    bec_ods.mrp_sr_assignments msra,
    bec_ods.mrp_sr_receipt_org s,
    bec_ods.mrp_sr_source_org  src,
	bec_ods.mtl_parameters mp
WHERE
    msr.sourcing_rule_id = msra.sourcing_rule_id
    and msra.assignment_set_id  = 1 
    and msra.sourcing_rule_id = s.sourcing_rule_id(+) 
    and s.sr_receipt_id   = src.sr_receipt_id(+) 
	AND src.source_organization_id = mp.organization_id(+)
			and (msra.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_mrp_sourcing_rules'
				and batch_name = 'po'))
        	)  ods 
     where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.sourcing_rule_id, 0) 
			|| '-' || nvl(ods.assignment_id,0)
			|| '-' || nvl(ods.SR_RECEIPT_ID,0) || '-' || nvl(ods.sr_source_id,0) 
			);

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_mrp_sourcing_rules'
	and batch_name = 'po';

commit;