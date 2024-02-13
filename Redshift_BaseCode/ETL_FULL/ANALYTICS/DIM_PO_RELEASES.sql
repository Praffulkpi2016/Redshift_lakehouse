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
drop table if exists bec_dwh.DIM_PO_RELEASES;

CREATE TABLE bec_dwh.DIM_PO_RELEASES 
diststyle all 
sortkey(PO_RELEASE_ID)
AS
(
SELECT
    PO_RELEASE_ID,
    PO_HEADER_ID,
    RELEASE_NUM,
    AGENT_ID,
    RELEASE_DATE,
    REVISION_NUM,
    REVISED_DATE,
    APPROVED_FLAG,
    APPROVED_DATE,
	  'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(PO_RELEASE_ID, 0) AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.PO_RELEASES_ALL
);

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_po_releases' and batch_name = 'ap';

COMMIT;