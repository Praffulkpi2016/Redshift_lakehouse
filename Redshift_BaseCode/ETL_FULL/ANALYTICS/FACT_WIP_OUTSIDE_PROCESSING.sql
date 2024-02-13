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

drop table if exists bec_dwh.FACT_WIP_OUTSIDE_PROCESSING;

create table bec_dwh.FACT_WIP_OUTSIDE_PROCESSING 
	diststyle all
	sortkey (PO_DISTRIBUTION_ID)
as
select
	WE.WIP_ENTITY_ID,
	DJ.PRIMARY_ITEM_ID,
	DJ.ORGANIZATION_ID,
	SI2.INVENTORY_ITEM_ID,
	POH.PO_HEADER_ID,
	POL.PO_LINE_ID,
	POLL.LINE_LOCATION_ID,
	POD.PO_DISTRIBUTION_ID,
	POR.PO_RELEASE_ID,
	BR.RESOURCE_ID,
	WE.WIP_ENTITY_NAME DISCRETE_JOB,
	DJ.DESCRIPTION JOB_DESCRIPTION,
	SI1.SEGMENT1 ASSEMBLY,
	SI1.DESCRIPTION ASSY_DESCRIPTION,
	SI1.PRIMARY_UOM_CODE UOM,
	DJ.START_QUANTITY SCHED_QUANTITY,	
	DJ.QUANTITY_COMPLETED QUANTITY_COMPLETED,
	TO_CHAR(DJ.SCHEDULED_START_DATE, 'DD-MON-YY') || TO_CHAR(DJ.SCHEDULED_START_DATE, ' HH24:MI') SCHED_START_DATE,
	TO_CHAR(DJ.SCHEDULED_COMPLETION_DATE, 'DD-MON-YY') || TO_CHAR(DJ.SCHEDULED_COMPLETION_DATE, ' HH24:MI') SCHED_COMPLETION_DATE,
	0 RATE,
	LU1.MEANING STATUS,
	LU2.MEANING JOB_TYPE,
	DJ.STATUS_TYPE,
	POD.WIP_OPERATION_SEQ_NUM as OP_SEQ,
	POD.WIP_RESOURCE_SEQ_NUM as SEQ_NUM,
	BR.RESOURCE_CODE RESOURCE_CODE ,
	POH.SEGMENT1 PO_NUMBER,
	POR.RELEASE_NUM RELEASE_NUM,
	MEV.FULL_NAME BUYER,
	--HP.PARTY_NAME VENDOR_NAME,
	APS.VENDOR_NAME,
	POL.LINE_NUM PO_LINE,
	UOM.UOM_CODE UNIT_OF_MEAS,
	POLL.SHIPMENT_NUM PO_SHIPMENT,
	NVL(POLL.PROMISED_DATE, POLL.NEED_BY_DATE) DUE_DATE,
	POD.QUANTITY_ORDERED QTY_ORDERED,
	POD.QUANTITY_DELIVERED QTY_DELIVERED,
	'N' AS IS_DELETED_FLG,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || WE.WIP_ENTITY_ID as WIP_ENTITY_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || SI2.INVENTORY_ITEM_ID as INVENTORY_ITEM_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || DJ.ORGANIZATION_ID as ORGANIZATION_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || DJ.PRIMARY_ITEM_ID as PRIMARY_ITEM_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || POH.PO_HEADER_ID as PO_HEADER_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || POL.PO_LINE_ID as PO_LINE_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || POLL.LINE_LOCATION_ID as LINE_LOCATION_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || POD.PO_DISTRIBUTION_ID as PO_DISTRIBUTION_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || POR.PO_RELEASE_ID as PO_RELEASE_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || BR.RESOURCE_ID as RESOURCE_ID_KEY,
	'N' as is_deleted_flag,
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
		source_system = 'EBS')|| '-' || nvl(POD.PO_DISTRIBUTION_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
			(select * from bec_ods.WIP_DISCRETE_JOBS where is_deleted_flg <> 'Y') DJ,
			(select * from bec_ods.WIP_ENTITIES where is_deleted_flg <> 'Y') WE,
			(select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg <> 'Y') SI2,
			(select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg <> 'Y') LU1,
			(select * from bec_ods.FND_LOOKUP_VALUES where is_deleted_flg <> 'Y') LU2,
			(select * from bec_ods.PO_HEADERS_ALL where is_deleted_flg <> 'Y') POH,
			(select * from bec_ods.MTL_UNITS_OF_MEASURE_TL where is_deleted_flg <> 'Y') UOM,
			(select * from bec_ods.PO_LINES_ALL where is_deleted_flg <> 'Y') POL,
			(select * from bec_ods.PO_LINE_LOCATIONS_ALL where is_deleted_flg <> 'Y') POLL,
			(select * from bec_ods.PO_DISTRIBUTIONS_ALL where is_deleted_flg <> 'Y') POD,
	        (select * from bec_ods.AP_SUPPLIERS where is_deleted_flg <> 'Y') APS,
			(select * from bec_ods.PO_RELEASES_ALL where is_deleted_flg <> 'Y') POR,
			(
			select
				ORG.ORGANIZATION_ID,
				P.PERSON_ID as EMPLOYEE_ID,
				P.FULL_NAME
			from
				(select * from bec_ods.PER_ALL_PEOPLE_F where is_deleted_flg <> 'Y') P,
				(select * from bec_ods.HR_ALL_ORGANIZATION_UNITS where is_deleted_flg <> 'Y') ORG
			where
				P.BUSINESS_GROUP_ID + 0 = ORG.BUSINESS_GROUP_ID
				and TRUNC(SYSDATE) between P.EFFECTIVE_START_DATE 
				and P.EFFECTIVE_END_DATE
				and P.EMPLOYEE_NUMBER is not null) MEV,
			(select * from bec_ods.BOM_RESOURCES where is_deleted_flg <> 'Y') BR,
			(select * from bec_ods.MTL_SYSTEM_ITEMS_B where is_deleted_flg <> 'Y') SI1,
			(select * from bec_ods.HR_ORGANIZATION_INFORMATION where is_deleted_flg <> 'Y') OOG
where
	1 = 1
	and DJ.ORGANIZATION_ID = OOG.ORGANIZATION_ID
	and DJ.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
	and DJ.ORGANIZATION_ID = WE.ORGANIZATION_ID
	and WE.WIP_ENTITY_ID = POD.WIP_ENTITY_ID
	and WE.ENTITY_TYPE != 2
	and DJ.ORGANIZATION_ID = SI1.ORGANIZATION_ID(+)
	and DJ.PRIMARY_ITEM_ID = SI1.INVENTORY_ITEM_ID(+)
	and LU1.LOOKUP_TYPE = 'WIP_JOB_STATUS'
	and LU1.LOOKUP_CODE = DJ.STATUS_TYPE
	and LU2.LOOKUP_TYPE = 'WIP_DISCRETE_JOB'
	and LU2.LOOKUP_CODE = DJ.JOB_TYPE
	and DJ.ORGANIZATION_ID = POD.DESTINATION_ORGANIZATION_ID
	and NVL(POD.ORG_ID::VARCHAR, NVL(OOG.ORG_INFORMATION3, '-1')) = NVL(OOG.ORG_INFORMATION3, '-1')
	and UPPER(OOG.ORG_INFORMATION_CONTEXT) = 'ACCOUNTING INFORMATION'
	and POD.DESTINATION_TYPE_CODE = 'SHOP FLOOR'
	and POD.WIP_ENTITY_ID is not null
	and POH.PO_HEADER_ID = POD.PO_HEADER_ID
	and POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID
	and POH.PO_HEADER_ID = POL.PO_HEADER_ID
	and POL.PO_LINE_ID = POLL.PO_LINE_ID
	and POH.PO_HEADER_ID = POLL.PO_HEADER_ID
	and DECODE(NVL(POD.PO_RELEASE_ID, -1), -1, POH.AGENT_ID, POR.AGENT_ID) = MEV.EMPLOYEE_ID
	and MEV.ORGANIZATION_ID = POD.DESTINATION_ORGANIZATION_ID
	and POR.PO_RELEASE_ID (+) = POD.PO_RELEASE_ID
	and POR.PO_HEADER_ID (+) = POD.PO_HEADER_ID
	--and POH.VENDOR_ID = HP.PARTY_ID (+)
	and POH.VENDOR_ID = APS.VENDOR_ID(+)
	and BR.RESOURCE_ID (+) = POD.BOM_RESOURCE_ID
	and SI2.INVENTORY_ITEM_ID = POL.ITEM_ID
	and SI2.ORGANIZATION_ID = POD.DESTINATION_ORGANIZATION_ID
	and UOM.UNIT_OF_MEASURE = POL.UNIT_MEAS_LOOKUP_CODE
;

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_wip_outside_processing'
	and batch_name = 'wip';

commit;