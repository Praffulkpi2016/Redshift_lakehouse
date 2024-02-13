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

drop table if exists bec_dwh.FACT_CSF_DEBRIEF_HEADERS;

create table bec_dwh.FACT_CSF_DEBRIEF_HEADERS distkey(debrief_header_id) sortkey(debrief_header_id) as 
SELECT 
  jtb.owner_type_code, 
  jtb.owner_id,
  cdh.debrief_header_id,
  cdh.debrief_number,
  cdh.debrief_date,
  cdh.debrief_status_id,
  jtb.task_id,
  jtb.base_task_id,
  cdh.task_assignment_id,
  cdh.processed_flag,
  jtb.scheduled_start_date,
  cdh.creation_date,
  cdh.last_update_date,
  cdh.created_by,
  cdh.last_updated_by,
  cdh.last_update_login,
  cdh.attribute_category,
  cdh.attribute1,
  cdh.attribute2,
  cdh.attribute3,
  cdh.attribute4,
  cdh.attribute5,
  cdh.attribute6,
  cdh.attribute7,
  cdh.attribute8,
  cdh.attribute9,
  cdh.attribute10,
  cdh.attribute11,
  cdh.attribute12,
  cdh.attribute13,
  cdh.attribute14,
  cdh.attribute15,
  jtb.task_number,
  jtb.task_priority_id,
  jtb.creation_date task_creation_date,
  jtb.source_object_type_code,
  jtb.task_type_id,
  jtv.task_name,
  jtv.description,
  jts1.NAME task_status1,
  jts2.NAME task_status2,
  jpa.party_id,
  jpa.party_name,
  jpa.party_number,
  jpa.address1
	  || ', '
	  || jpa.address2
	  || ', '
	  || jpa.address3
	  || ', '
	  || jpa.address4  ADDRESS,
  jpa.city || ', ' || jpa.state || ' - ' || jpa.postal_code CITY_STATE_ZIP,
  jta.resource_id,
  --csf_util_pvt.get_object_name (jta.resource_type_code,jta.resource_id) RESOURCE_NAME, 
  jta.resource_type_code,
  cia.incident_number,
  cia.incident_id,
  cia.incident_date,
  cia.customer_po_number,
  cia.creation_date sr_creation_date,
  cia.summary sr_summary,
  cia.incident_type_id,
  cia.incident_severity_id,
  jtb.source_object_id,
  cia.customer_id,
  jtb.cust_account_id,
  cia.SHIP_TO_SITE_ID ,
  cia.BILL_TO_SITE_ID,
  cia.incident_status_id,
  cii.INSTALL_LOCATION_ID,
  cii.location_id,
  cii.install_date,
  cii.external_reference padid,
  cii.INSTANCE_ID,
  cii.instance_number,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jtb.owner_id owner_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cdh.debrief_header_id debrief_header_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jtb.task_id task_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||jta.resource_id resource_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cia.incident_id incident_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cia.customer_id customer_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cii.location_id location_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||cii.INSTANCE_ID INSTANCE_ID_KEY,
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
  ) || '-' || nvl(cdh.debrief_header_id, 0)	as dw_load_id,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date
FROM
	(select task_status_id,NAME from bec_ods.jtf_task_statuses_tl where is_deleted_flg <> 'Y'
	and language      = 'US') jts1,
	(select task_status_id,NAME from bec_ods.jtf_task_statuses_tl where is_deleted_flg <> 'Y'
	and language      = 'US') jts2,
	(select * from bec_ods.hz_parties where is_deleted_flg <> 'Y') jpa,
	(select * from bec_ods.cs_incidents_all_b where is_deleted_flg <> 'Y') cia,
	(select * from bec_ods.csf_debrief_headers where is_deleted_flg <> 'Y') cdh,
	(select task_name,task_id,description from bec_ods.jtf_tasks_tl where is_deleted_flg <> 'Y'
	and language = 'US') jtv,
    (select * from bec_ods.jtf_tasks_b where is_deleted_flg <> 'Y') jtb,
	(select * from bec_ods.csi_item_instances where is_deleted_flg <> 'Y') cii,
	(select task_assignment_id ,task_id,assignee_role,assignment_status_id,
resource_id,resource_type_code	from bec_ods.jtf_task_assignments where is_deleted_flg <> 'Y') jta
WHERE   jtb.task_id = jtv.task_id
	AND jtb.source_object_id = cia.incident_id(+)
	AND cdh.task_assignment_id = jta.task_assignment_id
	AND jta.task_id = jtb.task_id
	AND jta.assignee_role = 'ASSIGNEE'
	AND NVL (jtb.deleted_flag,'N') != 'Y'
	AND jtb.customer_id = jpa.party_id(+)
	AND jtb.task_status_id = jts1.task_status_id
	AND jta.assignment_status_id = jts2.task_status_id
	AND cia.CUSTOMER_PRODUCT_ID = cii.INSTANCE_ID(+)
;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_csf_debrief_headers' 
  and batch_name = 'inv';
commit;