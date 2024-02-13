/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
--delete records
delete from bec_dwh.DIM_PERIODS
where (nvl(period_set_name, 'NA'),nvl(period_name, 'NA')) in 
(
select ods.period_set_name,ods.period_name
from bec_dwh.DIM_PERIODS dw,
(
select nvl(period_set_name, 'NA') as period_set_name,
nvl(period_name, 'NA') as period_name
FROM
bec_ods.gl_periods
where 1=1
and kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_periods' and batch_name = 'ap')
)ods
where dw.dw_load_id =
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(ods.PERIOD_SET_NAME, 'NA') || '-' || nvl(ods.PERIOD_NAME, 'NA')
);
commit;

--insert records
insert into bec_dwh.DIM_PERIODS
( SELECT GP.PERIOD_SET_NAME,
  GP.PERIOD_TYPE,
  GP.PERIOD_NAME,
  UPPER (GP.PERIOD_NAME) "GL_PERIOD",
  GP.PERIOD_NAME  "GL_PERIOD_LINK",
  GP.PERIOD_NUM "PERIOD_NUMBER",
  GP.PERIOD_YEAR,
  GP.QUARTER_NUM,
  GP.PERIOD_YEAR||'Q'||GP.QUARTER_NUM||'M'||GP.PERIOD_NUM "PERIOD_YYYY_QQ_MM",
  GP.ENTERED_PERIOD_NAME,
  GP.YEAR_START_DATE,
  ADD_MONTHS (GP.YEAR_START_DATE, 12 ) - 1 "YEAR_END_DATE",
  GP.QUARTER_START_DATE,
  ADD_MONTHS (GP.QUARTER_START_DATE, 3 ) - 1 "QUARTER_END_DATE",
  GP.START_DATE,
  GP.END_DATE,
  UPPER (GP.ENTERED_PERIOD_NAME )  || '-'  || GP.PERIOD_YEAR "PERIOD_YYYY",
  UPPER (GP.ENTERED_PERIOD_NAME )  || '-'  || GP.PERIOD_YEAR "PERIOD_NAME_YYYY",
  1 "SORT_KEY",
  ADJUSTMENT_PERIOD_FLAG,
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
       || nvl(PERIOD_SET_NAME, 'NA') || '-' || nvl(PERIOD_NAME, 'NA') AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.gl_periods GP
where 1=1
and GP.kca_seq_date > (
select (executebegints-prune_days)
from bec_etl_ctrl.batch_dw_info
where dw_table_name = 'dim_periods' and batch_name = 'ap')
and gp.is_deleted_flg<>'Y'
);
commit; 

-- soft delete
update bec_dwh.dim_periods set is_deleted_flg = 'Y'
where (nvl(period_set_name, 'NA'),nvl(period_name, 'NA')) 
not in 
(select ods.period_set_name,ods.period_name
from bec_dwh.dim_periods dw,
(
select nvl(period_set_name, 'NA') as period_set_name,
nvl(period_name, 'NA') as period_name
FROM
bec_ods.gl_periods
where 1=1
and is_deleted_flg <> 'Y'
)ods
where dw.dw_load_id =
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(ods.PERIOD_SET_NAME, 'NA') || '-' || nvl(ods.PERIOD_NAME, 'NA')
	   );
commit;
END;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_periods'
and batch_name = 'ap';

COMMIT;