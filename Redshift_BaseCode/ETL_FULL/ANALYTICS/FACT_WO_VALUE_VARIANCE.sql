/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_WO_VALUE_VARIANCE;

create table bec_dwh.FACT_WO_VALUE_VARIANCE diststyle all sortkey(organization_id,
inventory_item_id,primary_item_id,wip_entity_id)
as
(
select
	*,row_number() over(partition by dw_load_id) Rownumber
from
	bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG
union all
select
	*,row_number() over(partition by dw_load_id) Rownumber
from
	bec_dwh.FACT_WO_VALUE_VARIANCE_UNION2_STG
union all
select
	*,row_number() over(partition by dw_load_id) Rownumber
from
	bec_dwh.FACT_WO_VALUE_VARIANCE_UNION3_STG
union all
select
	*,row_number() over(partition by dw_load_id) Rownumber
from
	bec_dwh.FACT_WO_VALUE_VARIANCE_UNION4_STG
union all
select
	*,row_number() over(partition by dw_load_id) Rownumber
from
	bec_dwh.FACT_WO_VALUE_VARIANCE_UNION5_STG
union all
select
	*
from
	bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG
); 
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'fact_wo_value_variance' and batch_name = 'wip';

COMMIT;