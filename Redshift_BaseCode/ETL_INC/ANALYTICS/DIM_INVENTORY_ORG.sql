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
delete from bec_dwh.DIM_INVENTORY_ORG
where nvl(INV_ORGANIZATION_ID,0) in (
select nvl(ods.INV_ORGANIZATION_ID,0) as INV_ORGANIZATION_ID from bec_dwh.DIM_INVENTORY_ORG dw, 
(
SELECT HOU.ORGANIZATION_ID 	INV_ORGANIZATION_ID
FROM 
  bec_ods.HR_ALL_ORGANIZATION_UNITS HOU,
  bec_ods.HR_ORGANIZATION_INFORMATION HOI1,
  bec_ods.HR_ORGANIZATION_INFORMATION HOI2,
  bec_ods.MTL_PARAMETERS MP,
  bec_ods.GL_LEDGERS LGR,
  bec_ods.org_organization_definitions ood,
  bec_ods.hr_operating_units hou1
WHERE HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
AND HOU.ORGANIZATION_ID   = HOI2.ORGANIZATION_ID
AND HOU.ORGANIZATION_ID   = MP.ORGANIZATION_ID
AND HOI1.ORG_INFORMATION1 = 'INV'
AND HOI1.ORG_INFORMATION2 = 'Y'
AND ( HOI1.ORG_INFORMATION_CONTEXT
  || '') = 'CLASS'
AND ( HOI2.ORG_INFORMATION_CONTEXT
  || '') = 'Accounting Information'
AND DECODE(RTRIM(TRANSLATE(HOI2.ORG_INFORMATION1,'0123456789',' ')), NULL,-99999, cast(HOI2.ORG_INFORMATION1 as integer)) = LGR.LEDGER_ID
AND LGR.OBJECT_TYPE_CODE ='L'
AND NVL(LGR.COMPLETE_FLAG,'Y') ='Y'
and ood.ORGANIZATION_ID=mp.ORGANIZATION_ID
and ood.operating_unit = hou1.organization_id
and (HOU.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_inventory_org' and batch_name = 'ap')
 
)
) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.INV_ORGANIZATION_ID,0) 
);
commit;

-- Insert records

INSERT INTO bec_dwh.DIM_INVENTORY_ORG
(
inv_organization_id
,business_group_id
,enable_date
,disable_date
,inv_organization_code
,inv_organization_name
,set_of_books_id
,chart_of_accounts_id
,inventory_enabled_flag
,operating_unit
,currency_code
,operating_unit_name
,legal_entity
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
(
SELECT HOU.ORGANIZATION_ID 	INV_ORGANIZATION_ID,
  HOU.BUSINESS_GROUP_ID,
  HOU.DATE_FROM	ENABLE_DATE,
  HOU.DATE_TO 	DISABLE_DATE,
  MP.ORGANIZATION_CODE	INV_ORGANIZATION_CODE,
  HOU.NAME 	INV_ORGANIZATION_NAME,
  LGR.LEDGER_ID	SET_OF_BOOKS_ID,
  LGR.CHART_OF_ACCOUNTS_ID 	CHART_OF_ACCOUNTS_ID,
  HOI1.ORG_INFORMATION2 INVENTORY_ENABLED_FLAG,
  ood.operating_unit,
  LGR.CURRENCY_CODE,
  hou1."name" as operating_unit_name,
  DECODE(HOI2.ORG_INFORMATION_CONTEXT, 
		'Accounting Information', 
		CAST(HOI2.ORG_INFORMATION2 as integer), 
		NULL) as LEGAL_ENTITY,
		-- audit columns
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(HOU.organization_id,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date 
FROM 
  bec_ods.HR_ALL_ORGANIZATION_UNITS HOU,
  bec_ods.HR_ORGANIZATION_INFORMATION HOI1,
  bec_ods.HR_ORGANIZATION_INFORMATION HOI2,
  bec_ods.MTL_PARAMETERS MP,
  bec_ods.GL_LEDGERS LGR,
  bec_ods.org_organization_definitions ood,
  bec_ods.hr_operating_units hou1
WHERE HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
AND HOU.ORGANIZATION_ID   = HOI2.ORGANIZATION_ID
AND HOU.ORGANIZATION_ID   = MP.ORGANIZATION_ID
AND HOI1.ORG_INFORMATION1 = 'INV'
AND HOI1.ORG_INFORMATION2 = 'Y'
AND ( HOI1.ORG_INFORMATION_CONTEXT
  || '') = 'CLASS'
AND ( HOI2.ORG_INFORMATION_CONTEXT
  || '') = 'Accounting Information'
AND DECODE(RTRIM(TRANSLATE(HOI2.ORG_INFORMATION1,'0123456789',' ')), NULL,-99999, cast(HOI2.ORG_INFORMATION1 as integer)) = LGR.LEDGER_ID
AND LGR.OBJECT_TYPE_CODE ='L'
AND NVL(LGR.COMPLETE_FLAG,'Y') ='Y'
and ood.ORGANIZATION_ID=mp.ORGANIZATION_ID
and ood.operating_unit = hou1.organization_id
and (HOU.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_inventory_org' and batch_name = 'ap')
 
)
 );
-- Soft delete

update bec_dwh.dim_inventory_org set is_deleted_flg = 'Y'
where nvl(INV_ORGANIZATION_ID,0) not in (
select nvl(ods.INV_ORGANIZATION_ID,0) as INV_ORGANIZATION_ID from bec_dwh.DIM_INVENTORY_ORG dw, 
(
SELECT HOU.ORGANIZATION_ID as INV_ORGANIZATION_ID
FROM 
  (select * from bec_ods.HR_ALL_ORGANIZATION_UNITS where is_deleted_flg <> 'Y')HOU,
  (select * from bec_ods.HR_ORGANIZATION_INFORMATION where is_deleted_flg <> 'Y') HOI1,
  (select * from bec_ods.HR_ORGANIZATION_INFORMATION where is_deleted_flg <> 'Y') HOI2,
  (select * from bec_ods.MTL_PARAMETERS where is_deleted_flg <> 'Y') MP,
  (select * from bec_ods.GL_LEDGERS where is_deleted_flg <> 'Y') LGR,
  (select * from bec_ods.org_organization_definitions where is_deleted_flg <> 'Y') ood,
  (select * from bec_ods.hr_operating_units where is_deleted_flg <> 'Y') hou1
WHERE HOU.ORGANIZATION_ID = HOI1.ORGANIZATION_ID
AND HOU.ORGANIZATION_ID   = HOI2.ORGANIZATION_ID
AND HOU.ORGANIZATION_ID   = MP.ORGANIZATION_ID
AND HOI1.ORG_INFORMATION1 = 'INV'
AND HOI1.ORG_INFORMATION2 = 'Y'
AND ( HOI1.ORG_INFORMATION_CONTEXT
  || '') = 'CLASS'
AND ( HOI2.ORG_INFORMATION_CONTEXT
  || '') = 'Accounting Information'
AND DECODE(RTRIM(TRANSLATE(HOI2.ORG_INFORMATION1,'0123456789',' ')), NULL,-99999, cast(HOI2.ORG_INFORMATION1 as integer)) = LGR.LEDGER_ID
AND LGR.OBJECT_TYPE_CODE ='L'
AND NVL(LGR.COMPLETE_FLAG,'Y') ='Y'
and ood.ORGANIZATION_ID=mp.ORGANIZATION_ID
and ood.operating_unit = hou1.organization_id
) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.INV_ORGANIZATION_ID,0) 
);

commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inventory_org'
and batch_name = 'ap';

COMMIT;