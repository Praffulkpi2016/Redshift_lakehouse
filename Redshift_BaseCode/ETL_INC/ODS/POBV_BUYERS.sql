/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/

begin;

TRUNCATE TABLE bec_ods.POBV_BUYERS;
 

INSERT INTO bec_ods.POBV_BUYERS (
   buyer_id,
	buyer_employee_number,
	buyer_name,
	authorization_limit,
	start_effective_date,
	end_effective_date,
	category_id,
	location_id,
	created_date,
	created_by,
	last_updated_date,
	last_updated_by,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
     buyer_id,
	buyer_employee_number,
	buyer_name,
	authorization_limit,
	start_effective_date,
	end_effective_date,
	category_id,
	location_id,
	created_date,
	created_by,
	last_updated_date,
	last_updated_by,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,  
	KCA_SEQ_DATE
    FROM
        bec_ods_stg.POBV_BUYERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'pobv_buyers';