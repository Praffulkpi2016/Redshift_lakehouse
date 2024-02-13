/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full incremental approach for ODS.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V
(
	plan_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,customer_site_id NUMERIC(15,0)   ENCODE az64
	,customer_name VARCHAR(1000)   ENCODE lzo
	,customer_site_name VARCHAR(40)   ENCODE lzo
	,supplier_id NUMERIC(15,0)   ENCODE az64
	,supplier_site_id NUMERIC(15,0)   ENCODE az64
	,supplier_name VARCHAR(1000)   ENCODE lzo
	,supplier_site_name VARCHAR(40)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,item_name VARCHAR(250)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,order_details VARCHAR(4000)   ENCODE lzo
	,planner_code VARCHAR(10)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,vmi_type VARCHAR(12)   ENCODE lzo
	,aps_supplier_id NUMERIC(15,0)   ENCODE az64
	,aps_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,aps_customer_id NUMERIC(15,0)   ENCODE az64
	,aps_customer_site_id NUMERIC(15,0)   ENCODE az64
	,buyer_code VARCHAR(4000)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V
(
plan_id,
	sr_instance_id,
	customer_id,
	customer_site_id,
	customer_name,
	customer_site_name,
	supplier_id,
	supplier_site_id,
	supplier_name,
	supplier_site_name,
	inventory_item_id,
	item_name,
	description,
	order_details,
	planner_code,
	organization_id,
	vmi_type,
	aps_supplier_id,
	aps_supplier_site_id,
	aps_customer_id,
	aps_customer_site_id,
	buyer_code,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
select 
	plan_id,
	sr_instance_id,
	customer_id,
	customer_site_id,
	customer_name,
	customer_site_name,
	supplier_id,
	supplier_site_id,
	supplier_name,
	supplier_site_name,
	inventory_item_id,
	item_name,
	description,
	order_details,
	planner_code,
	organization_id,
	vmi_type,
	aps_supplier_id,
	aps_supplier_site_id,
	aps_customer_id,
	aps_customer_site_id,
	buyer_code,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.MSC_SUP_DEM_ENTRIES_VMI_V
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='msc_sup_dem_entries_vmi_v'; 

commit;