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

DROP TABLE if exists bec_ods.xxbec_pegging_data;

CREATE TABLE IF NOT EXISTS bec_ods.xxbec_pegging_data
(
	level_seq NUMERIC(15,0)   ENCODE az64
	,plan_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,pegging_id NUMERIC(15,0)   ENCODE az64
	,prev_pegging_id NUMERIC(15,0)   ENCODE az64
	,parent_pegging_id NUMERIC(15,0)   ENCODE az64
	,demand_id NUMERIC(15,0)   ENCODE az64
	,transaction_id NUMERIC(15,0)   ENCODE az64
	,msc_trxn_id NUMERIC(15,0)   ENCODE az64
	,demand_qty NUMERIC(28,10)   ENCODE az64
	,demand_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,item_id NUMERIC(15,0)   ENCODE az64
	,item_org VARCHAR(4000)   ENCODE lzo
	,pegged_qty NUMERIC(28,10)   ENCODE az64
	,origination_name VARCHAR(4000)   ENCODE lzo
	,end_pegged_qty NUMERIC(28,10)   ENCODE az64
	,end_demand_qty NUMERIC(28,10)   ENCODE az64
	,end_demand_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_item_org VARCHAR(4000)   ENCODE lzo
	,end_origination_name VARCHAR(4000)   ENCODE lzo
	,end_satisfied_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_disposition VARCHAR(4000)   ENCODE lzo
	,order_type NUMERIC(15,0)   ENCODE az64
	,end_demand_class VARCHAR(34)   ENCODE lzo
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,op_seq_num NUMERIC(15,0)   ENCODE az64
	,item_desc_org VARCHAR(4000)   ENCODE lzo
	,new_order_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_dock_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,new_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.xxbec_pegging_data (
 	level_seq,
	plan_id,
	organization_id,
	pegging_id,
	prev_pegging_id,
	parent_pegging_id,
	demand_id,
	transaction_id,
	msc_trxn_id,
	demand_qty,
	demand_date,
	item_id,
	item_org,
	pegged_qty,
	origination_name,
	end_pegged_qty,
	end_demand_qty,
	end_demand_date,
	end_item_org,
	end_origination_name,
	end_satisfied_date,
	end_disposition,
	order_type,
	end_demand_class,
	sr_instance_id,
	op_seq_num,
	item_desc_org,
	new_order_date,
	new_dock_date,
	new_start_date,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	level_seq,
	plan_id,
	organization_id,
	pegging_id,
	prev_pegging_id,
	parent_pegging_id,
	demand_id,
	transaction_id,
	msc_trxn_id,
	demand_qty,
	demand_date,
	item_id,
	item_org,
	pegged_qty,
	origination_name,
	end_pegged_qty,
	end_demand_qty,
	end_demand_date,
	end_item_org,
	end_origination_name,
	end_satisfied_date,
	end_disposition,
	order_type,
	end_demand_class,
	sr_instance_id,
	op_seq_num,
	item_desc_org,
	new_order_date,
	new_dock_date,
	new_start_date,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.XXBEC_PEGGING_DATA;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xxbec_pegging_data';
	
commit;