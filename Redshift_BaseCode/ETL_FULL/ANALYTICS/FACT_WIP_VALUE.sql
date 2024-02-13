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

drop table if exists bec_dwh.FACT_WIP_VALUE;

create table bec_dwh.FACT_WIP_VALUE diststyle all sortkey(wip_entity_id,organization_id)
as
(
SELECT
	W.WIP_ENTITY_ID,
	W.ORGANIZATION_ID,
	W.PRIMARY_ITEM_ID,
	SG.SCHEDULE_GROUP_ID,
	E.WIP_ENTITY_NAME JOB_NUMBER,
	M.SEGMENT1 ASSEMBLY_ITEM,
	M.DESCRIPTION ASSEMBLY_ITEM_DESCRIPTION,
	W.CREATION_DATE JOB_CREATION_DATE,
	W.DATE_RELEASED,
	W.SCHEDULED_START_DATE,
	W.SCHEDULED_COMPLETION_DATE,
	W.DATE_CLOSED,
	U.USER_NAME JOB_CREATED_BY,
	WRO.OPERATION_SEQ_NUM ,
	ML.MEANING CLASS_TYPE,
	W.CLASS_CODE,
	ML2.MEANING PRELIM_STATUS,
	SG.SCHEDULE_GROUP_NAME,
	W.START_QUANTITY,
	W.QUANTITY_COMPLETED,
	W.QUANTITY_SCRAPPED,
	(W.START_QUANTITY - W.QUANTITY_COMPLETED - W.QUANTITY_SCRAPPED ) QUANTITY_END_BALANCE,
	W.NET_QUANTITY,
	W.SCHEDULED_COMPLETION_DATE SCHEDULED_END_DATE,
	O.ORGANIZATION_CODE,
 	SUM(P.TL_RESOURCE_IN + P.TL_OVERHEAD_IN + P.TL_OUTSIDE_PROCESSING_IN +
           P.PL_MATERIAL_IN + P.PL_RESOURCE_IN + P.PL_OVERHEAD_IN +
           P.PL_OUTSIDE_PROCESSING_IN + P.PL_MATERIAL_OVERHEAD_IN) COSTS_INCURRED,
	SUM(P.TL_RESOURCE_OUT + P.TL_OVERHEAD_OUT +
           P.TL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OUT +
           P.PL_RESOURCE_OUT + P.PL_OVERHEAD_OUT +
           P.PL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OVERHEAD_OUT) COSTS_RELIEVED,
	SUM(P.TL_RESOURCE_VAR + P.TL_OVERHEAD_VAR +
           P.TL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_VAR +
           P.PL_RESOURCE_VAR + P.PL_OVERHEAD_VAR +
           P.PL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_OVERHEAD_VAR) VARIANCES_RELIEVED,
	SUM(P.TL_RESOURCE_IN + P.TL_OVERHEAD_IN + P.TL_OUTSIDE_PROCESSING_IN +
           P.PL_MATERIAL_IN + P.PL_RESOURCE_IN + P.PL_OVERHEAD_IN +
           P.PL_OUTSIDE_PROCESSING_IN + P.PL_MATERIAL_OVERHEAD_IN -
           (P.TL_RESOURCE_OUT + P.TL_OVERHEAD_OUT +
           P.TL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OUT +
           P.PL_RESOURCE_OUT + P.PL_OVERHEAD_OUT +
           P.PL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OVERHEAD_OUT) -
           (P.TL_RESOURCE_VAR + P.TL_OVERHEAD_VAR +
           P.TL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_VAR +
           P.PL_RESOURCE_VAR + P.PL_OVERHEAD_VAR +
           P.PL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_OVERHEAD_VAR)) END_VARIANCE,
	'N' AS IS_DELETED_FLG,
	(
	SELECT
		SYSTEM_ID
	FROM
		BEC_ETL_CTRL.ETLSOURCEAPPID
	WHERE
		SOURCE_SYSTEM = 'EBS'
    ) AS SOURCE_APP_ID,
	(
	SELECT
		SYSTEM_ID
	FROM
		BEC_ETL_CTRL.ETLSOURCEAPPID
	WHERE
		SOURCE_SYSTEM = 'EBS'
    )
    || '-' || NVL(W.ORGANIZATION_ID, 0)
              || '-' || NVL(W.WIP_ENTITY_ID, 0) 
              || '-' || NVL(W.PRIMARY_ITEM_ID, 0)
               || '-' || NVL(WRO.OPERATION_SEQ_NUM, 0) AS DW_LOAD_ID,
	GETDATE() AS DW_INSERT_DATE,
	GETDATE() AS DW_UPDATE_DATE
FROM
	(SELECT * FROM BEC_ODS.WIP_PERIOD_BALANCES WHERE IS_DELETED_FLG <> 'Y') P,
	(SELECT * FROM BEC_ODS.WIP_DISCRETE_JOBS WHERE IS_DELETED_FLG <> 'Y') W,
	(SELECT * FROM BEC_ODS.WIP_ENTITIES WHERE IS_DELETED_FLG <> 'Y')  E,
	(SELECT * FROM BEC_ODS.WIP_REQUIREMENT_OPERATIONS WHERE IS_DELETED_FLG <> 'Y') WRO,
	(SELECT * FROM BEC_ODS.FND_USER WHERE IS_DELETED_FLG <> 'Y') U,
	(SELECT * FROM BEC_ODS.MTL_SYSTEM_ITEMS_B WHERE IS_DELETED_FLG <> 'Y') M,
	(SELECT * FROM BEC_ODS.WIP_SCHEDULE_GROUPS WHERE IS_DELETED_FLG <> 'Y') SG,
	(SELECT * FROM BEC_ODS.ORG_ORGANIZATION_DEFINITIONS WHERE IS_DELETED_FLG <> 'Y') O,
	(SELECT * FROM BEC_ODS.FND_LOOKUP_VALUES WHERE IS_DELETED_FLG <> 'Y') ML,
	(SELECT * FROM BEC_ODS.FND_LOOKUP_VALUES WHERE IS_DELETED_FLG <> 'Y') ML2
WHERE
	1 = 1
	AND W.WIP_ENTITY_ID = P.WIP_ENTITY_ID
	AND W.WIP_ENTITY_ID = E.WIP_ENTITY_ID
	AND W.ORGANIZATION_ID = E.ORGANIZATION_ID
	AND W.PRIMARY_ITEM_ID = M.INVENTORY_ITEM_ID (+)
	AND W.ORGANIZATION_ID = M.ORGANIZATION_ID (+)
	AND O.ORGANIZATION_ID = E.ORGANIZATION_ID
	AND O.ORGANIZATION_ID = P.ORGANIZATION_ID
	AND W.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID(+)
	AND W.ORGANIZATION_ID = WRO.ORGANIZATION_ID (+)
	AND W.PRIMARY_ITEM_ID = WRO.INVENTORY_ITEM_ID (+)
	AND W.ORGANIZATION_ID = SG.ORGANIZATION_ID (+)
	AND W.SCHEDULE_GROUP_ID = SG.SCHEDULE_GROUP_ID (+)
	AND ML.LANGUAGE = 'US'
	AND ML.LOOKUP_TYPE = 'WIP_CLASS_TYPE'
	AND ML.LOOKUP_CODE = P.CLASS_TYPE
	AND ML2.LANGUAGE = 'US'
	AND ML2.LOOKUP_CODE = W.STATUS_TYPE
	AND ML2.LOOKUP_TYPE = 'WIP_JOB_STATUS'
	AND E.CREATED_BY = U.USER_ID(+)
--	AND E.ORGANIZATION_ID IN (106)
--	AND E.WIP_ENTITY_NAME = 'PRG-141445-B28' 	--'TEC-B3-143893-MG5'
GROUP BY
	O.ORGANIZATION_CODE,
	E.WIP_ENTITY_NAME,
	W.CREATION_DATE,
	W.DATE_RELEASED,
	W.STATUS_TYPE,
	M.SEGMENT1,
	M.DESCRIPTION,
	W.CLASS_CODE,
	W.SCHEDULED_START_DATE,
	W.SCHEDULED_COMPLETION_DATE,
	W.START_QUANTITY,
	W.QUANTITY_COMPLETED,
	W.QUANTITY_SCRAPPED,
	W.STATUS_TYPE,
	U.USER_NAME,
	WRO.OPERATION_SEQ_NUM,
	ML.MEANING ,
	SG.SCHEDULE_GROUP_NAME,
	W.SCHEDULED_COMPLETION_DATE ,
	ML2.MEANING ,
	W.WIP_ENTITY_ID,
	W.ORGANIZATION_ID,
	W.DATE_CLOSED ,
	W.PRIMARY_ITEM_ID,
	SG.SCHEDULE_GROUP_ID,
	W.NET_QUANTITY
 );
end;
	
update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_wip_value'
	and batch_name = 'wip';

commit; 	             