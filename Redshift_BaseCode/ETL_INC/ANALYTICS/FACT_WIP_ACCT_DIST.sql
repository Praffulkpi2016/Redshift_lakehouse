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

delete
from
	bec_dwh.fact_wip_acct_dist
where
	(nvl(transaction_id, 0),nvl(wip_entity_id, 0),nvl(ORGANIZATION_ID, 0),nvl(wip_sub_ledger_id, 0),nvl(reference_account,0)) in (
	select
		nvl(ODS.transaction_id, 0) as transaction_id,
		nvl(ODS.wip_entity_id, 0) as wip_entity_id,
		nvl(ODS.ORGANIZATION_ID, 0) as ORGANIZATION_ID,
		nvl(ODS.wip_sub_ledger_id, 0) as wip_sub_ledger_id,
		nvl(ODS.reference_account,0) as reference_account
	from
		bec_dwh.fact_wip_acct_dist dw,
		(
SELECT    wta.transaction_id,
		  wta.wip_entity_id,
          wta.organization_id,
		  wta.wip_sub_ledger_id,
		  wta.reference_account
    FROM bec_ods.wip_transaction_accounts  wta,
		 (SELECT 
		WT.TRANSACTION_ID,
		WT.LAST_UPDATE_DATE,
		WT.ORGANIZATION_ID,
		WT.WIP_ENTITY_ID,
		DECODE(WT.TRANSACTION_TYPE
		, 2
		, NULL
		, ML3.MEANING) STANDARD_RATE_MEANING,
		WE.WIP_ENTITY_NAME,
		(
					SELECT
						department_code
					FROM
						bec_ods.bom_departments 
					WHERE
						department_id = decode(we.entity_type, 6, 
						nvl(wt.charge_department_id, wt.department_id), 7, nvl(wt.charge_department_id,
						wt.department_id), wt.department_id)
		) DEPARTMENT_CODE,
		ML1.MEANING TRANSACTION_TYPE_MEANING,
		WT.OPERATION_SEQ_NUM,
		WT.RESOURCE_SEQ_NUM,
		ML1.is_deleted_flg,
		ML3.is_deleted_flg is_deleted_flg1,
		WT.is_deleted_flg is_deleted_flg2,
		WE.is_deleted_flg is_deleted_flg3
		FROM
		BEC_ODS.FND_LOOKUP_VALUES  ML1,
		BEC_ODS.FND_LOOKUP_VALUES  ML3,
		BEC_ODS.WIP_TRANSACTIONS  WT,
		BEC_ODS.WIP_ENTITIES  WE
		WHERE 1=1
		--AND BD.DEPARTMENT_ID = WT.DEPARTMENT_ID
		AND ML1.LOOKUP_TYPE = 'WIP_TRANSACTION_TYPE'
		AND ML1.LOOKUP_CODE = WT.TRANSACTION_TYPE
		AND ML3.LOOKUP_TYPE = 'SYS_YES_NO'
		AND ML3.LOOKUP_CODE = NVL(WT.STANDARD_RATE_FLAG,1)
		AND WT.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
		) wtv,
		  BEC_ODS.wip_discrete_jobs  w,
		  BEC_ODS.bom_resources  br
    WHERE wta.transaction_id  = wtv.transaction_id
      AND wta.organization_id = wtv.organization_id
	  AND wta.wip_entity_id   = w.wip_entity_id(+)
	  and wta.resource_id     = br.resource_id(+)
	  AND (wta.kca_seq_date > (select (executebegints-prune_days) 
	  from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_wip_acct_dist' and batch_name = 'costing')
	  OR wta.IS_DELETED_FLG = 'Y'
	  OR wtv.IS_DELETED_FLG = 'Y'
	  OR wtv.IS_DELETED_FLG1 = 'Y'
	  OR wtv.IS_DELETED_FLG2 = 'Y'
	  OR wtv.IS_DELETED_FLG3 = 'Y'
	  OR w.IS_DELETED_FLG = 'Y'
	  OR br.IS_DELETED_FLG = 'Y')
		    ) ods
	where
		dw.dw_load_id = 	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')
		|| '-' || nvl(ODS.transaction_id, 0)
		|| '-' || nvl(ODS.wip_entity_id, 0)	
		|| '-' || nvl(ODS.ORGANIZATION_ID, 0)
		|| '-' || nvl(ODS.wip_sub_ledger_id, 0)		
		|| '-' || nvl(ODS.reference_account,0)
);

commit;
-- Insert records

insert
	into
	bec_dwh.fact_wip_acct_dist 
(
	transaction_id,
	wip_entity_id,
	inventory_item_id,
	organization_id,
	wip_sub_ledger_id,
	reference_account,
	wip_entity_name,
	class_code,
	transaction_date,
	transaction_type_meaning,
	operation_seq_num,
	department_code,
	resource_seq_num,
	resource_code,
	resource_uom,
	SUBINVENTORY,
	basis,
	activity,
	standard_rate_meaning,
	rate_or_amount,
	primary_quantity,
	base_transaction_value,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date

	)
(SELECT    wta.transaction_id,
		  wta.wip_entity_id,
          w.primary_item_id inventory_item_id,
          wta.organization_id,
		  wta.wip_sub_ledger_id,
		  wta.reference_account,
          wtv.wip_entity_name,
		  w.class_code,
          wta.transaction_date, 
		  wtv.transaction_type_meaning,
          wtv.operation_seq_num, 
		  wtv.department_code, 
		  wtv.resource_seq_num,
          br.resource_code,
          br.unit_of_measure resource_uom,
		  W.COMPLETION_SUBINVENTORY AS SUBINVENTORY,
          DECODE (wta.basis_type,
                  1, 'Item',
                  2, 'Lot',
                  3, 'Resource Units',
                  4, 'Resource Value',
                  5, 'Total Value',
                  6, 'Activity',
                  cast(wta.basis_type as varchar(30)) ) basis,
          DECODE (wta.activity_id,
                  1, 'Run',
                  2, 'Prerun',
                  3, 'Postrun',
                  4, 'Move',
                  5, 'Queue',
                  cast(wta.activity_id  as VARCHAR(30))) activity,
          wtv.standard_rate_meaning, 
		  wta.rate_or_amount rate_or_amount,
          wta.primary_quantity, 
		  wta.base_transaction_value,
		  'N' as is_deleted_flg,
			(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')		
		|| '-' || nvl(wta.transaction_id, 0)
		|| '-' || nvl(wta.wip_entity_id, 0)	
		|| '-' || nvl(wta.ORGANIZATION_ID, 0)
		|| '-' || nvl(wta.wip_sub_ledger_id, 0)		
		|| '-' || nvl(wta.reference_account,0)
				as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
     FROM (SELECT * FROM bec_ods.wip_transaction_accounts WHERE is_deleted_flg <> 'Y') wta,
		 (SELECT 
		WT.TRANSACTION_ID,
		WT.LAST_UPDATE_DATE,
		WT.ORGANIZATION_ID,
		WT.WIP_ENTITY_ID,
		DECODE(WT.TRANSACTION_TYPE
		, 2
		, NULL
		, ML3.MEANING) STANDARD_RATE_MEANING,
		WE.WIP_ENTITY_NAME,
		(
					SELECT
						department_code
					FROM
						(SELECT * FROM bec_ods.bom_departments WHERE is_deleted_flg <> 'Y')
					WHERE
						department_id = decode(we.entity_type, 6, 
						nvl(wt.charge_department_id, wt.department_id), 7, nvl(wt.charge_department_id,
						wt.department_id), wt.department_id)
		) DEPARTMENT_CODE,
		ML1.MEANING TRANSACTION_TYPE_MEANING,
		WT.OPERATION_SEQ_NUM,
		WT.RESOURCE_SEQ_NUM
		FROM
		(SELECT * FROM BEC_ODS.FND_LOOKUP_VALUES WHERE is_deleted_flg <> 'Y') ML1,
		(SELECT * FROM BEC_ODS.FND_LOOKUP_VALUES WHERE is_deleted_flg <> 'Y') ML3,
		--BOM_DEPARTMENTS BD,
		(SELECT * FROM BEC_ODS.WIP_TRANSACTIONS WHERE is_deleted_flg <> 'Y') WT,
		(SELECT * FROM BEC_ODS.WIP_ENTITIES WHERE is_deleted_flg <> 'Y') WE
		WHERE 1=1
		--AND BD.DEPARTMENT_ID = WT.DEPARTMENT_ID
		AND ML1.LOOKUP_TYPE = 'WIP_TRANSACTION_TYPE'
		AND ML1.LOOKUP_CODE = WT.TRANSACTION_TYPE
		AND ML3.LOOKUP_TYPE = 'SYS_YES_NO'
		AND ML3.LOOKUP_CODE = NVL(WT.STANDARD_RATE_FLAG,1)
		AND WT.WIP_ENTITY_ID = WE.WIP_ENTITY_ID) wtv,
		  (SELECT * FROM BEC_ODS.wip_discrete_jobs WHERE is_deleted_flg <> 'Y') w,
		  (SELECT * FROM BEC_ODS.bom_resources WHERE is_deleted_flg <> 'Y') br
    WHERE wta.transaction_id  = wtv.transaction_id
      AND wta.organization_id = wtv.organization_id
	  AND wta.wip_entity_id   = w.wip_entity_id(+)
	  and wta.resource_id     = br.resource_id(+)
	  AND (wta.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_wip_acct_dist' and batch_name = 'costing'))	);
	  
commit;


end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_wip_acct_dist'
	and batch_name = 'costing';

COMMIT;