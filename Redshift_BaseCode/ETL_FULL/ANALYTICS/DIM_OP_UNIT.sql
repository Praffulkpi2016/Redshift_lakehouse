/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
drop table if exists bec_dwh.DIM_OP_UNIT;

CREATE TABLE bec_dwh.dim_op_unit 
diststyle all 
sortkey(organization_id)
AS(
SELECT
    DECODE(hop.business_group_id, 0, NULL, hop.business_group_id) AS business_group_id,
    trunc(hop.organization_id) AS organization_id,
    hop.name                       AS op_unit_name,
    set_of_books_id,
    gl.name                        AS ledger_name,
    gl.short_name                  AS ledger_short_name,
    gl.currency_code               AS ledger_currency_code,
    hou.name                       AS legal_entity_name,
    hou.attribute1                 AS attribute1,
    hou.attribute2                 AS attribute2,
    hou.attribute3                 AS attribute3,
    hou.attribute4                 AS attribute4,
    hou.attribute5                 AS attribute5,
	hop.short_code,
    'N' as is_deleted_flg,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
    (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(hop.organization_id,0) as dw_load_id,
    getdate() as dw_insert_date,
    getdate() as dw_update_date    
FROM
    bec_ods.hr_operating_units hop
    INNER JOIN bec_ods.hr_all_organization_units hou ON hop.organization_id = hou.organization_id
    INNER JOIN bec_ods.gl_ledgers gl ON hop.set_of_books_id = gl.ledger_id
	where 1=1
	and (hop.is_deleted_flg <> 'Y' or hou.is_deleted_flg <> 'Y' or gl.is_deleted_flg <> 'Y' )
);

END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_op_unit'
and batch_name = 'ap';

COMMIT;