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

DROP TABLE if exists bec_ods.OE_ORDER_SOURCES;

CREATE TABLE IF NOT EXISTS bec_ods.OE_ORDER_SOURCES
(

     order_source_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(240)   ENCODE lzo
	,description VARCHAR(2000)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,create_customers_flag VARCHAR(1)   ENCODE lzo
	,use_ids_flag VARCHAR(1)   ENCODE lzo
	,aia_enabled_flag VARCHAR(1)   ENCODE lzo
	,zd_edition_name VARCHAR(30)   ENCODE lzo 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

insert
	into
	bec_ods.OE_ORDER_SOURCES (
    order_source_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	"name",
	description,
	enabled_flag,
	create_customers_flag,
	use_ids_flag,
	aia_enabled_flag,
	zd_edition_name, 
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    select
	order_source_id,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
	"name",
	description,
	enabled_flag,
	create_customers_flag,
	use_ids_flag,
	aia_enabled_flag,
	zd_edition_name,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
from
	bec_ods_stg.OE_ORDER_SOURCES;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'oe_order_sources';

commit;