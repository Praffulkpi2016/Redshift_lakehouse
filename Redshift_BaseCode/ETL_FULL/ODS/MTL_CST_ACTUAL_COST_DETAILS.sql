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

DROP TABLE if exists bec_ods.MTL_CST_ACTUAL_COST_DETAILS;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_CST_ACTUAL_COST_DETAILS
(
	layer_id NUMERIC(15,0)   ENCODE az64
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,cost_element_id NUMERIC(15,0)   ENCODE az64
	,level_type NUMERIC(15,0)   ENCODE az64
	,transaction_action_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,actual_cost NUMERIC(28,10)   ENCODE az64
	,prior_cost NUMERIC(28,10)   ENCODE az64
	,new_cost NUMERIC(28,10)   ENCODE az64
	,insertion_flag VARCHAR(1)   ENCODE lzo
	,variance_amount NUMERIC(28,10)   ENCODE az64
	,user_entered VARCHAR(1)   ENCODE lzo
	,transaction_costed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payback_variance_amount NUMERIC(28,10)   ENCODE az64
	,onhand_variance_amount NUMERIC(28,10)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_CST_ACTUAL_COST_DETAILS (
	layer_id,
	transaction_id,
	organization_id,
	cost_element_id,
	level_type,
	transaction_action_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	inventory_item_id,
	actual_cost,
	prior_cost,
	new_cost,
	insertion_flag,
	variance_amount,
	user_entered,
	transaction_costed_date,
	payback_variance_amount,
	onhand_variance_amount,
	kca_operation,
	is_deleted_flg,
	kca_seq_id ,
	kca_seq_date)
SELECT
	layer_id,
	transaction_id,
	organization_id,
	cost_element_id,
	level_type,
	transaction_action_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	inventory_item_id,
	actual_cost,
	prior_cost,
	new_cost,
	insertion_flag,
	variance_amount,
	user_entered,
	transaction_costed_date,
	payback_variance_amount,
	onhand_variance_amount,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_cst_actual_cost_details';