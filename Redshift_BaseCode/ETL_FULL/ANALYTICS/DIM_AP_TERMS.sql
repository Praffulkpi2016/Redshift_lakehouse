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
drop table if exists bec_dwh.DIM_AP_TERMS;

CREATE TABLE bec_dwh.DIM_AP_TERMS 
diststyle all 
sortkey(TERM_ID)
AS
( SELECT
    term_id,
    type               "AP_TERM_TYPE",
    start_date_active,
    end_date_active,
    enabled_flag,
    name               "AP_TERM",
    description        "AP_TERM_DESC",
	'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                  AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    ||'-'|| nvl(term_id, 0) AS dw_load_id,
    getdate()          AS dw_insert_date,
    getdate()          AS dw_update_date
FROM
    bec_ods.ap_terms_tl
where language = 'US'
);

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ap_terms' and batch_name = 'ap';

COMMIT;

