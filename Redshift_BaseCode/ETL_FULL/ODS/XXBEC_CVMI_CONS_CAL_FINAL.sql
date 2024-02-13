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

DROP TABLE if exists bec_ods.XXBEC_CVMI_CONS_CAL_FINAL;

CREATE TABLE IF NOT EXISTS bec_ods.XXBEC_CVMI_CONS_CAL_FINAL
(
	 plan_name VARCHAR(60)   ENCODE lzo
	,ran_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,as_of_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,part_number VARCHAR(60)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,demand NUMERIC(28,10)   ENCODE az64
	,bqoh NUMERIC(28,10)   ENCODE az64
	,cvmi_qoh NUMERIC(28,10)   ENCODE az64
	,ncvmi_receipts NUMERIC(28,10)   ENCODE az64
	,cvmi_receipts NUMERIC(28,10)   ENCODE az64
	,remaining NUMERIC(28,10)   ENCODE az64
	,demand_pulls NUMERIC(28,10)   ENCODE az64
	,aging_pulls NUMERIC(28,10)   ENCODE az64
	,future_consumptions NUMERIC(28,10)   ENCODE az64
	,ending_bqoh NUMERIC(28,10)   ENCODE az64
	,ending_cvmi_bqoh NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.XXBEC_CVMI_CONS_CAL_FINAL (
     plan_name
	,ran_date  
	,as_of_date 
	,part_number 
	,organization_id 
	,demand 
	,bqoh 
	,cvmi_qoh 
	,ncvmi_receipts
	,cvmi_receipts 
	,remaining 
	,demand_pulls 
	,aging_pulls 
	,future_consumptions 
	,ending_bqoh
	,ending_cvmi_bqoh 
	,KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
     plan_name
	,ran_date  
	,as_of_date 
	,part_number 
	,organization_id 
	,demand 
	,bqoh 
	,cvmi_qoh 
	,ncvmi_receipts
	,cvmi_receipts 
	,remaining 
	,demand_pulls 
	,aging_pulls 
	,future_consumptions 
	,ending_bqoh
	,ending_cvmi_bqoh 
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.XXBEC_CVMI_CONS_CAL_FINAL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xxbec_cvmi_cons_cal_final';
	
commit;