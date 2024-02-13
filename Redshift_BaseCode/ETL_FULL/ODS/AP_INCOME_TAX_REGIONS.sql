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

drop table if exists bec_ods.ap_income_tax_regions;

create table if not exists bec_ods.ap_income_tax_regions
(
	region_short_name VARCHAR(10) ENCODE lzo,
region_long_name VARCHAR(25) ENCODE lzo,
region_code numeric(15,0) ENCODE az64,
reporting_limit numeric(15,0) ENCODE az64,
num_of_payees numeric(15,0) ENCODE az64,
control_total1 numeric(28,10) ENCODE az64,
control_total2 numeric(28,10) ENCODE az64,
control_total3 numeric(28,10) ENCODE az64,
control_total4 numeric(28,10) ENCODE az64,
control_total5 numeric(28,10) ENCODE az64,
control_total6 numeric(28,10) ENCODE az64,
control_total7 numeric(28,10) ENCODE az64,
control_total8 numeric(28,10) ENCODE az64,
control_total9 numeric(28,10) ENCODE az64,
control_total10 numeric(28,10) ENCODE az64,
active_date TIMESTAMP without TIME zone ENCODE az64,
inactive_date TIMESTAMP without TIME zone ENCODE az64,
last_update_date TIMESTAMP without TIME zone ENCODE az64,
last_updated_by numeric(15,0) ENCODE az64,
last_update_login numeric(15,0) ENCODE az64,
creation_date TIMESTAMP without TIME zone ENCODE az64,
created_by numeric(15,0) ENCODE az64,
reporting_limit_method_code VARCHAR(25) ENCODE lzo,
control_total13 numeric(28,10) ENCODE az64,
control_total13e numeric(28,10) ENCODE az64,
control_total14 numeric(28,10) ENCODE az64,
control_total15a numeric(28,10) ENCODE az64,
control_total15b numeric(28,10) ENCODE az64,
KCA_OPERATION VARCHAR(10)   ENCODE lzo,
IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
diststyle auto
;

insert into	bec_ods.ap_income_tax_regions
    (region_short_name,
	region_long_name,
	region_code,
	reporting_limit,
	num_of_payees,
	control_total1,
	control_total2,
	control_total3,
	control_total4,
	control_total5,
	control_total6,
	control_total7,
	control_total8,
	control_total9,
	control_total10,
	active_date,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	reporting_limit_method_code,
	control_total13,
	control_total13e,
	control_total14,
	control_total15a,
	control_total15b,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
select
	region_short_name,
	region_long_name,
	region_code,
	reporting_limit,
	num_of_payees,
	control_total1,
	control_total2,
	control_total3,
	control_total4,
	control_total5,
	control_total6,
	control_total7,
	control_total8,
	control_total9,
	control_total10,
	active_date,
	inactive_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	creation_date,
	created_by,
	reporting_limit_method_code,
	control_total13,
	control_total13e,
	control_total14,
	control_total15a,
	control_total15b,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.ap_income_tax_regions;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'ap_income_tax_regions';
	
commit;