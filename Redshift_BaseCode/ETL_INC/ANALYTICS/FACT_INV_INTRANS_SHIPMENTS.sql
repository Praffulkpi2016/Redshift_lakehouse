/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;
 --delete records from temp table
TRUNCATE bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP;
--Insert records into temp table
INSERT INTO bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP
(SELECT  distinct   mmt1.transaction_id, 
            mmt1.inventory_item_id,
			mmt1.organization_id
       FROM bec_ods.mtl_material_transactions  mmt1,
            bec_ods.mtl_material_transactions  mtl3,
            bec_ods.cst_item_costs  cic
      WHERE 1=1
	    --and mmt1.transaction_type_id = 21
        --AND mmt1.transaction_source_type_id = 13
        and mmt1.INVENTORY_ITEM_ID = cic.INVENTORY_ITEM_ID
        and mmt1.ORGANIZATION_ID = cic.ORGANIZATION_ID
        and cic.COST_TYPE_ID = 1
        AND mtl3.transfer_transaction_id(+) = mmt1.transaction_id
		AND (mmt1.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_inv_intrans_shipments' and batch_name = 'inv')
         OR 
		 cic.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_inv_intrans_shipments' and batch_name = 'inv')));
--delete records from fact table
delete from bec_dwh.FACT_INV_INTRANS_SHIPMENTS
WHERE exists (select 1 from bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP tmp
              where tmp.transaction_id = FACT_INV_INTRANS_SHIPMENTS.transaction_id
			  and tmp.inventory_item_id = FACT_INV_INTRANS_SHIPMENTS.inventory_item_id
			  and tmp.organization_id = FACT_INV_INTRANS_SHIPMENTS.organization_id
             );
--insert records into fact table
Insert into bec_dwh.FACT_INV_INTRANS_SHIPMENTS
( 
		select 
			 transaction_id, 
             inventory_item_id,
			 transaction_type_id,
			 transaction_source_type_id,
              "Transaction Type",
              "Transaction Source",
             "Shipment Number",
            "Transaction Date", 
             "Shipped Quantity",
             TOTAL_RCV, 
			 organization_id,
			 transfer_organization_id,
             ITEM_COST,
			 ext_cost
		,(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || transaction_id as transaction_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || transaction_type_id as transaction_type_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || transaction_source_type_id as transaction_source_type_id_key,
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_key,
		-- audit columns
		'N' as is_deleted_flg,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
			source_system = 'EBS'
		) as source_app_id,
		  (
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
			source_system = 'EBS'
			)
			|| '-' || nvl(transaction_id, 0)  
		as dw_load_id,  
		getdate() as dw_insert_date,
		getdate() as dw_update_date
		from
		(
			SELECT      mmt1.transaction_id, 
            mmt1.inventory_item_id,
			mmt1.transaction_type_id,
			mmt1.transaction_source_type_id,
            DECODE (mmt1.transaction_type_id,
                    '21', 'Intransit Shipment'
                   ) "Transaction Type",
            DECODE (mmt1.transaction_source_type_id,
                    '13', 'Inventory'
                   ) "Transaction Source",
            mmt1.shipment_number "Shipment Number",
            mmt1.transaction_date "Transaction Date", 
            mmt1.transaction_quantity * -1 "Shipped Quantity",
            0 TOTAL_RCV, 
			mmt1.organization_id,
			mmt1.transfer_organization_id,
            cic.ITEM_COST,
			mmt1.transaction_quantity*-1*cic.ITEM_COST ext_cost
       FROM (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'y'
	         ) mmt1,
            (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'y') mtl3,
            (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'y') cic,
			bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP tmp
      WHERE mmt1.transaction_type_id = 21
        AND mmt1.transaction_source_type_id = 13
        and mmt1.INVENTORY_ITEM_ID = cic.INVENTORY_ITEM_ID
        and mmt1.ORGANIZATION_ID = cic.ORGANIZATION_ID
        and cic.COST_TYPE_ID = 1
        AND mtl3.transfer_transaction_id(+) = mmt1.transaction_id
        AND NOT EXISTS (
               SELECT 1
                 FROM (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt2
                WHERE mmt2.transfer_transaction_id = mmt1.transaction_id
                  AND mmt2.shipment_number = mmt1.shipment_number
                  AND mmt2.transaction_type_id = 12
                  AND mmt2.transaction_source_type_id = 13)
		and mmt1.transaction_id = tmp.transaction_id
			  and mmt1.inventory_item_id = tmp.inventory_item_id
			  and mmt1.organization_id = tmp.organization_id
   GROUP BY mmt1.transaction_id, 
            mmt1.inventory_item_id,
			mmt1.transaction_type_id,
			mmt1.transaction_source_type_id,
            mmt1.transaction_type_id,
            mmt1.transaction_source_type_id,
            mmt1.shipment_number,
            mmt1.transaction_date,
            mmt1.transaction_quantity * -1,
            mmt1.organization_id,
			mmt1.transfer_organization_id,
            cic.item_cost
   UNION
   SELECT   mmt1.transaction_id,
            mmt1.inventory_item_id,
			mmt1.transaction_type_id,
			mmt1.transaction_source_type_id,
            DECODE (mmt1.transaction_type_id,
                    '21', 'Intransit Shipment'
                   ) "Transaction Type",
            DECODE (mmt1.transaction_source_type_id,
                    '13', 'Inventory'
                   ) "Transaction Source",
            mmt1.shipment_number "Shipment Number",
            mmt1.transaction_date "Transaction Date", 
            mmt1.transaction_quantity * -1 "Shipped Quantity",
            SUM (mtl3.transaction_quantity) TOTAL_RCV,
            mmt1.organization_id, 
			mmt1.transfer_organization_id,
             cic.ITEM_COST,
			 mmt1.transaction_quantity*-1*cic.ITEM_COST ext_cost
       FROM (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt1,
            (select transfer_transaction_id,transaction_quantity from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mtl3,
            (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic,
			bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP tmp
      WHERE mmt1.transaction_type_id = 21
        AND mmt1.transaction_source_type_id = 13
        and mmt1.INVENTORY_ITEM_ID = cic.INVENTORY_ITEM_ID
        and mmt1.ORGANIZATION_ID = cic.ORGANIZATION_ID
        and cic.COST_TYPE_ID = 1
        AND mtl3.transfer_transaction_id = mmt1.transaction_id
        AND mmt1.transaction_quantity * -1 >
               (SELECT   SUM (transaction_quantity)
                    FROM (select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt2
                   WHERE mmt2.transfer_transaction_id = mmt1.transaction_id
                     AND mmt2.shipment_number = mmt1.shipment_number
                     AND mmt2.transaction_type_id = 12
                     AND mmt2.transaction_source_type_id = 13
                GROUP BY mmt2.transfer_transaction_id)
		and mmt1.transaction_id = tmp.transaction_id
			  and mmt1.inventory_item_id = tmp.inventory_item_id
			  and mmt1.organization_id = tmp.organization_id
   GROUP BY mmt1.transaction_id, 
            mmt1.inventory_item_id, 
			mmt1.transaction_type_id,
			mmt1.transaction_source_type_id,
            mmt1.transaction_type_id,
            mmt1.transaction_source_type_id,
            mmt1.shipment_number,
            mmt1.transaction_date,
            mmt1.transaction_quantity * -1,
            mmt1.organization_id,
			mmt1.transfer_organization_id,
            cic.item_cost 
		)
	);
	
END;
update 
bec_etl_ctrl.batch_dw_info 
set 
load_type = 'I', 
last_refresh_date = getdate() 
where 
dw_table_name = 'fact_inv_intrans_shipments' 
and batch_name = 'inv';
commit;
