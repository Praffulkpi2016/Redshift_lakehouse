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

DROP TABLE if exists bec_ods.HR_OPERATING_UNITS;

CREATE TABLE IF NOT EXISTS bec_ods.HR_OPERATING_UNITS
(    business_group_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(240)   ENCODE lzo
	,date_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,date_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,short_code VARCHAR(150)   ENCODE lzo
	,set_of_books_id VARCHAR(150)   ENCODE lzo
	,default_legal_context_id VARCHAR(150)   ENCODE lzo
	,usable_flag VARCHAR(150)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;


insert into bec_ods.HR_OPERATING_UNITS
(
business_group_id,
	organization_id,
	"name",
	date_from,
	date_to,
	short_code,
	set_of_books_id,
	default_legal_context_id,
	usable_flag,
kca_operation 
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date)
SELECT

business_group_id,
	organization_id,
	"name",
	date_from,
	date_to,
	short_code,
	set_of_books_id,
	default_legal_context_id,
	usable_flag,
kca_operation 
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
FROM bec_ods_stg.HR_OPERATING_UNITS;

end;
update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'hr_operating_units';
commit;