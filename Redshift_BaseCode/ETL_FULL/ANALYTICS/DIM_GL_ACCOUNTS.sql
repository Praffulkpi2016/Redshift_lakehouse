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
drop table if exists bec_dwh.DIM_GL_ACCOUNTS;

CREATE TABLE  bec_dwh.DIM_GL_ACCOUNTS distkey(CODE_COMBINATION_ID) sortkey(CODE_COMBINATION_ID)
AS
( SELECT CODE_COMBINATION_ID,
  CHART_OF_ACCOUNTS_ID,
  GL_ACCOUNT_TYPE,
  CONCATENATED_SEGMENTS,
--  ACCOUNT_TYPE,
  GCC.SEGMENT1 segment1,
  (SELECT F2A.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1A,bec_ods.FND_FLEX_VALUES_TL F2A,bec_ods.FND_ID_FLEX_SEGMENTS F3A
WHERE F1A.FLEX_VALUE_ID=F2A.FLEX_VALUE_ID AND F2A.LANGUAGE='US'
 AND F1A.FLEX_VALUE_SET_ID=F3A.FLEX_VALUE_SET_ID
 AND F3A.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3A.ID_FLEX_CODE='GL#'
 AND F3A.APPLICATION_COLUMN_NAME='SEGMENT1'
 AND F1A.FLEX_VALUE=GCC.SEGMENT1
 AND F3A.APPLICATION_ID=101
) AS SEGMENT1_DESC,
GCC.SEGMENT2 SEGMENT2,
(SELECT F2B.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1B,bec_ods.FND_FLEX_VALUES_TL F2B,bec_ods.FND_ID_FLEX_SEGMENTS F3B
WHERE F1B.FLEX_VALUE_ID=F2B.FLEX_VALUE_ID AND F2B.LANGUAGE='US'
 AND F1B.FLEX_VALUE_SET_ID=F3B.FLEX_VALUE_SET_ID
 AND F3B.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3B.ID_FLEX_CODE='GL#'
 AND F3B.APPLICATION_COLUMN_NAME='SEGMENT2'
 AND F1B.FLEX_VALUE=GCC.SEGMENT2
 AND F3B.APPLICATION_ID=101
) AS SEGMENT2_DESC,
GCC.SEGMENT3 SEGMENT3,
(SELECT F2C.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1C,bec_ods.FND_FLEX_VALUES_TL F2C,bec_ods.FND_ID_FLEX_SEGMENTS F3C
WHERE F1C.FLEX_VALUE_ID=F2C.FLEX_VALUE_ID AND F2C.LANGUAGE='US'
 AND F1C.FLEX_VALUE_SET_ID=F3C.FLEX_VALUE_SET_ID
 AND F3C.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3C.ID_FLEX_CODE='GL#'
 AND F3C.APPLICATION_COLUMN_NAME='SEGMENT3'
 AND F1C.FLEX_VALUE=GCC.SEGMENT3
 AND F3C.APPLICATION_ID=101
) AS SEGMENT3_DESC,
GCC.SEGMENT4 SEGMENT4,
(SELECT F2D.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1D,bec_ods.FND_FLEX_VALUES_TL F2D,bec_ods.FND_ID_FLEX_SEGMENTS F3D
WHERE F1D.FLEX_VALUE_ID=F2D.FLEX_VALUE_ID AND F2D.LANGUAGE='US'
 AND F1D.FLEX_VALUE_SET_ID=F3D.FLEX_VALUE_SET_ID
 AND F3D.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3D.ID_FLEX_CODE='GL#'
 AND F3D.APPLICATION_COLUMN_NAME='SEGMENT4'
 AND F1D.FLEX_VALUE=GCC.SEGMENT4
 AND F3D.APPLICATION_ID=101
) AS SEGMENT4_DESC,
GCC.SEGMENT5 SEGMENT5,
(SELECT F2E.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1E,bec_ods.FND_FLEX_VALUES_TL F2E,bec_ods.FND_ID_FLEX_SEGMENTS F3E
WHERE F1E.FLEX_VALUE_ID=F2E.FLEX_VALUE_ID AND F2E.LANGUAGE='US'
 AND F1E.FLEX_VALUE_SET_ID=F3E.FLEX_VALUE_SET_ID
 AND F3E.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3E.ID_FLEX_CODE='GL#'
 AND F3E.APPLICATION_COLUMN_NAME='SEGMENT5'
 AND F1E.FLEX_VALUE=GCC.SEGMENT5
 AND F3E.APPLICATION_ID=101
) AS SEGMENT5_DESC,
GCC.SEGMENT6 SEGMENT6,
(SELECT F2F.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1F,bec_ods.FND_FLEX_VALUES_TL F2F,bec_ods.FND_ID_FLEX_SEGMENTS F3F
WHERE F1F.FLEX_VALUE_ID=F2F.FLEX_VALUE_ID AND F2F.LANGUAGE='US'
 AND F1F.FLEX_VALUE_SET_ID=F3F.FLEX_VALUE_SET_ID
 AND F3F.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3F.ID_FLEX_CODE='GL#'
 AND F3F.APPLICATION_COLUMN_NAME='SEGMENT6'
 AND F1F.FLEX_VALUE=GCC.SEGMENT6
 AND F3F.APPLICATION_ID=101
) AS SEGMENT6_DESC,
GCC.SEGMENT7 SEGMENT7,
(SELECT F2G.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1G,bec_ods.FND_FLEX_VALUES_TL F2G,bec_ods.FND_ID_FLEX_SEGMENTS F3G
WHERE F1G.FLEX_VALUE_ID=F2G.FLEX_VALUE_ID AND F2G.LANGUAGE='US'
 AND F1G.FLEX_VALUE_SET_ID=F3G.FLEX_VALUE_SET_ID
 AND F3G.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3G.ID_FLEX_CODE='GL#'
 AND F3G.APPLICATION_COLUMN_NAME='SEGMENT7'
 AND F1G.FLEX_VALUE=GCC.SEGMENT7
 AND F3G.APPLICATION_ID=101
) AS SEGMENT7_DESC,
GCC.SEGMENT8 SEGMENT8,
(SELECT F2H.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1H,bec_ods.FND_FLEX_VALUES_TL F2H,bec_ods.FND_ID_FLEX_SEGMENTS F3H
WHERE F1H.FLEX_VALUE_ID=F2H.FLEX_VALUE_ID AND F2H.LANGUAGE='US'
 AND F1H.FLEX_VALUE_SET_ID=F3H.FLEX_VALUE_SET_ID
 AND F3H.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3H.ID_FLEX_CODE='GL#'
 AND F3H.APPLICATION_COLUMN_NAME='SEGMENT8'
 AND F1H.FLEX_VALUE=GCC.SEGMENT8
 AND F3H.APPLICATION_ID=101
) AS SEGMENT8_DESC,
GCC.SEGMENT9 SEGMENT9,
(SELECT F2I.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1I,bec_ods.FND_FLEX_VALUES_TL F2I,bec_ods.FND_ID_FLEX_SEGMENTS F3I
WHERE F1I.FLEX_VALUE_ID=F2I.FLEX_VALUE_ID AND F2I.LANGUAGE='US'
 AND F1I.FLEX_VALUE_SET_ID=F3I.FLEX_VALUE_SET_ID
 AND F3I.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3I.ID_FLEX_CODE='GL#'
 AND F3I.APPLICATION_COLUMN_NAME='SEGMENT9'
 AND F1I.FLEX_VALUE=GCC.SEGMENT9
 AND F3I.APPLICATION_ID=101
) AS SEGMENT9_DESC,
GCC.SEGMENT10 SEGMENT10,
(SELECT F2J.DESCRIPTION
FROM bec_ods.FND_FLEX_VALUES F1J,bec_ods.FND_FLEX_VALUES_TL F2J,bec_ods.FND_ID_FLEX_SEGMENTS F3J
WHERE F1J.FLEX_VALUE_ID=F2J.FLEX_VALUE_ID AND F2J.LANGUAGE='US'
 AND F1J.FLEX_VALUE_SET_ID=F3J.FLEX_VALUE_SET_ID
 AND F3J.ID_FLEX_NUM=GCC.CHART_OF_ACCOUNTS_ID
 AND F3J.ID_FLEX_CODE='GL#'
 AND F3J.APPLICATION_COLUMN_NAME='SEGMENT10'
 AND F1J.FLEX_VALUE=GCC.SEGMENT10
 AND F3J.APPLICATION_ID=101
) AS SEGMENT10_DESC,
  ENABLED_FLAG,
  SUMMARY_FLAG,
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
	|| '-' || nvl(CODE_COMBINATION_ID, 0)  AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
FROM
    bec_ods.GL_CODE_COMBINATIONS_KFV GCC
);
end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_gl_accounts' and batch_name = 'ap';

COMMIT;