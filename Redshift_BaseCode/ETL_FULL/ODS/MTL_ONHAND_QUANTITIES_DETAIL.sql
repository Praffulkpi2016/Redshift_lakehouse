/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.MTL_ONHAND_QUANTITIES_DETAIL;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_ONHAND_QUANTITIES_DETAIL
(  
     inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,date_received TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,primary_transaction_quantity NUMERIC(28,10)   ENCODE az64
	,subinventory_code VARCHAR(10)   ENCODE lzo
	,revision VARCHAR(3)   ENCODE lzo
	,locator_id NUMERIC(15,0)   ENCODE az64
	,create_transaction_id NUMERIC(15,0)   ENCODE az64
	,update_transaction_id NUMERIC(15,0)   ENCODE az64
	,lot_number VARCHAR(80)   ENCODE lzo
	,orig_date_received TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,containerized_flag NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,onhand_quantities_id NUMERIC(15,0)   ENCODE az64
	,organization_type NUMERIC(15,0)   ENCODE az64
	,owning_organization_id NUMERIC(15,0)   ENCODE az64
	,owning_tp_type NUMERIC(15,0)   ENCODE az64
	,planning_organization_id NUMERIC(15,0)   ENCODE az64
	,planning_tp_type NUMERIC(15,0)   ENCODE az64
	,transaction_uom_code VARCHAR(3)   ENCODE lzo
	,transaction_quantity NUMERIC(28,10)   ENCODE az64
	,secondary_uom_code VARCHAR(3)   ENCODE lzo
	,secondary_transaction_quantity NUMERIC(28,10)   ENCODE az64
	,is_consigned NUMERIC(15,0)   ENCODE az64
	,lpn_id NUMERIC(15,0)   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,mcc_code VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_ONHAND_QUANTITIES_DETAIL (
    inventory_item_id,
	organization_id,
	date_received,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	primary_transaction_quantity,
	subinventory_code,
	revision,
	locator_id,
	create_transaction_id,
	update_transaction_id,
	lot_number,
	orig_date_received,
	cost_group_id,
	containerized_flag,
	project_id,
	task_id,
	onhand_quantities_id,
	organization_type,
	owning_organization_id,
	owning_tp_type,
	planning_organization_id,
	planning_tp_type,
	transaction_uom_code,
	transaction_quantity,
	secondary_uom_code,
	secondary_transaction_quantity,
	is_consigned,
	lpn_id,
	status_id,
	mcc_code,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
   inventory_item_id,
	organization_id,
	date_received,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	primary_transaction_quantity,
	subinventory_code,
	revision,
	locator_id,
	create_transaction_id,
	update_transaction_id,
	lot_number,
	orig_date_received,
	cost_group_id,
	containerized_flag,
	project_id,
	task_id,
	onhand_quantities_id,
	organization_type,
	owning_organization_id,
	owning_tp_type,
	planning_organization_id,
	planning_tp_type,
	transaction_uom_code,
	transaction_quantity,
	secondary_uom_code,
	secondary_transaction_quantity,
	is_consigned,
	lpn_id,
	status_id,
	mcc_code,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_onhand_quantities_detail';
	
commit;