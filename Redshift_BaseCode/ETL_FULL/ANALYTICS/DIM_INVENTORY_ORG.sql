/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_dwh.DIM_INVENTORY_ORG;

CREATE TABLE bec_dwh.DIM_INVENTORY_ORG 
diststyle all 
sortkey(INV_ORGANIZATION_ID)
AS (
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
 );
 
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_inventory_org'
and batch_name = 'ap';

COMMIT; 