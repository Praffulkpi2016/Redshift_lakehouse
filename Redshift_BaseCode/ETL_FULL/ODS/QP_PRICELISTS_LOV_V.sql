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
drop table if exists bec_ods.QP_PRICELISTS_LOV_V;

CREATE TABLE IF NOT EXISTS bec_ods.QP_PRICELISTS_LOV_V
(
	price_list_id NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(240)   ENCODE lzo
	,description VARCHAR(2000)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,qdt_start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,qdt_end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,currency_code VARCHAR(30)   ENCODE lzo
	,agreement_id NUMERIC(15,0)   ENCODE az64
	,sold_to_org_id NUMERIC(15,0)   ENCODE az64
	,source_system_code VARCHAR(30)   ENCODE lzo
	,shareable_flag VARCHAR(1)   ENCODE lzo
	,orig_system_header_ref VARCHAR(50)   ENCODE lzo
	,list_source_code VARCHAR(30)   ENCODE lzo
	,qry_type NUMERIC(15,0)   ENCODE az64
	,global_flag VARCHAR(1)   ENCODE lzo
	,orig_org_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.QP_PRICELISTS_LOV_V
(
price_list_id,
	"name",
	description,
	start_date_active,
	end_date_active,
	qdt_start_date_active,
	qdt_end_date_active,
	currency_code,
	agreement_id,
	sold_to_org_id,
	source_system_code,
	shareable_flag,
	orig_system_header_ref,
	list_source_code,
	qry_type,
	global_flag,
	orig_org_id 
	,kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
(
select 
	price_list_id,
	"name",
	description,
	start_date_active,
	end_date_active,
	qdt_start_date_active,
	qdt_end_date_active,
	currency_code,
	agreement_id,
	sold_to_org_id,
	source_system_code,
	shareable_flag,
	orig_system_header_ref,
	list_source_code,
	qry_type,
	global_flag,
	orig_org_id 
	,kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.QP_PRICELISTS_LOV_V
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='qp_pricelists_lov_v'; 

commit;