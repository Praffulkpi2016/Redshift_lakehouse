/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_INVENTORY_VALUE_RT;

create table bec_dwh_rpt.FACT_INVENTORY_VALUE_RT --3806330
	diststyle all as (
SELECT distinct xx.*
FROM BEC_ODS.XXBEC_INVENTORY_VALUE_RPT xx
 ) ;
 
 end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate() 
where
	dw_table_name = 'fact_inventory_value_rt'
	and batch_name = 'inv';

commit;