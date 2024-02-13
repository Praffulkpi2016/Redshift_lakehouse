/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for stage.
# File Version: KPI v1.0
*/
BEGIN; 

DROP TABLE IF EXISTS bec_ods_stg.bom_item_current_rev_view;

CREATE TABLE bec_ods_stg.bom_item_current_rev_view AS 
SELECT * FROM bec_raw_dl_ext.bom_item_current_rev_view
where kca_operation != 'DELETE';

END;
