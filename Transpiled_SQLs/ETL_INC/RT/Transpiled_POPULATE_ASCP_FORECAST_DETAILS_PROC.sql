CREATE OR REPLACE PROCEDURE POPULATE_ASCP_FORECAST_DETAILS(
  
)
LANGUAGE plpgsql AS
$$ 
DECLARE 
l_forecast_qty NUMERIC := 0;
l_remaining_forecast_qty NUMERIC;
l_available_ohq NUMERIC := 0;
l_used_ohq NUMERIC;
l_available_po_qty NUMERIC := 0;
l_used_po_qty NUMERIC := 0;
l_used_plan_qty NUMERIC := 0;
l_used_cons_po_qty NUMERIC := 0;
l_used_cons_plan_qty NUMERIC := 0;
l_available_plan_qty NUMERIC := 0;
l_first_iteration string := 'Y';
i_dates RECORD;
l_part_number string;
l_organization_id NUMERIC;
l_remaining_ohq NUMERIC;
l_remaining_po_qty NUMERIC;
l_remaining_plan_qty NUMERIC;
l_forecast_designator string;
l_forecast_date date;
l_different_designator string := 'N';
begin 
FOR i_dates in (
  select 
    forecast_designator, 
    organization_id, 
    part_number, 
    forecast_date, 
    sum(current_forecast_quantity) forecast_qty 
  from 
    gold_bec_dwh.fact_ascp_forecast 
  where 
    current_forecast_quantity > 0 
    and forecast_date >= getdate()
  group by 
	forecast_designator, 
    organization_id, 
    part_number, 
    forecast_date 
  order by 
    organization_id, 
    part_number, 
    forecast_date, 
    forecast_designator asc
) LOOP 
--For the first iteration
IF l_first_iteration = 'Y' THEN 
l_part_number := i_dates.part_number;
l_organization_id := i_dates.organization_id;
l_forecast_designator := i_dates.forecast_designator;
l_forecast_date := i_dates.forecast_date;
l_remaining_ohq := 0;
l_remaining_po_qty := 0;
l_remaining_plan_qty := 0;
l_used_po_qty := 0;
l_used_plan_qty := 0;
l_used_cons_po_qty := 0;
l_used_cons_plan_qty := 0;
l_used_ohq := 0;
END IF;
--For next   compare the items and organizations .
--If different initiate value l_first_iteration to Y
IF l_first_iteration = 'N' THEN IF 
l_part_number = i_dates.part_number 
AND l_organization_id = i_dates.organization_id THEN 
l_first_iteration := 'N';
ELSE --setting the variables to new item and first iteration as 'Y'
l_part_number := i_dates.part_number;
l_organization_id := i_dates.organization_id;
l_forecast_designator := i_dates.forecast_designator;
l_forecast_date := i_dates.forecast_date;
l_remaining_ohq := 0;
l_remaining_po_qty := 0;
l_remaining_plan_qty := 0;
l_used_po_qty := 0;
l_used_cons_po_qty := 0;
l_used_plan_qty := 0;
l_used_cons_plan_qty := 0;
l_used_ohq := 0;
l_first_iteration := 'Y';
END IF;
IF l_part_number = i_dates.part_number 
AND l_organization_id = i_dates.organization_id 
AND l_forecast_date = i_dates.forecast_date 
AND l_forecast_designator <> i_dates.forecast_designator THEN l_different_designator := 'Y';
l_forecast_designator := i_dates.forecast_designator;
ELSE l_different_designator := 'N';
END IF;
END IF;
l_forecast_qty := i_dates.forecast_qty;
--GET ONHANDQTY
IF l_first_iteration = 'Y' THEN --For the first iteration
select 
  nvl(
    sum(quantity), 
    0
  ) into l_available_ohq 
from 
  gold_bec_dwh.FACT_INV_OHQ_DETAILS 
where 
  part_number = i_dates.part_number 
  and organization_id = i_dates.organization_id;
ELSE l_available_ohq := l_remaining_ohq;
END IF;
l_remaining_ohq := l_available_ohq - l_forecast_qty;
IF l_remaining_ohq < 0 THEN l_remaining_ohq := 0;
END IF;
l_used_ohq := l_available_ohq - l_remaining_ohq;
l_remaining_forecast_qty := l_forecast_qty - l_used_ohq;
--GET PO QUANTITY
IF l_different_designator = 'N' THEN -- to handle a different designator for same date
select 
  nvl(
    sum(ship_quantity), 
    0
  ) - l_used_cons_po_qty into l_available_po_qty 
from 
  gold_bec_dwh.fact_open_po_data 
where 
  item = i_dates.part_number 
  and organization_id = i_dates.organization_id 
  and nvl(PROMISED_DATE, need_by_date) <= i_dates.forecast_date;
ELSE l_available_po_qty := l_remaining_po_qty;
END IF;
l_remaining_po_qty := l_available_po_qty - l_remaining_forecast_qty;
IF l_remaining_po_qty < 0 THEN l_remaining_po_qty := 0;
END IF;
l_used_po_qty := l_available_po_qty - l_remaining_po_qty;
l_remaining_forecast_qty := l_remaining_forecast_qty - l_used_po_qty;
l_used_cons_po_qty := l_used_cons_po_qty + l_used_po_qty;
--GET PLANNED ORDER QUANTITY--Plan id 
IF l_different_designator = 'N' THEN 
select 
  nvl(
    sum(quantity_rate), 
    0
  ) - l_used_cons_plan_qty into l_available_plan_qty 
from 
  gold_bec_dwh.fact_ascp_supply_demand fasd 
where 
  plan_id = 40029 
  AND sr_instance_id = 2021 
  AND category_set_id = 9 
  AND item_segments = i_dates.part_number 
  and organization_id = i_dates.organization_id 
  and new_due_date <= i_dates.forecast_date 
  and order_type_text = 'Planned order';
ELSE l_available_plan_qty := l_remaining_plan_qty;
END IF;
l_remaining_plan_qty := l_available_plan_qty - l_remaining_forecast_qty;
IF l_remaining_plan_qty < 0 THEN l_remaining_plan_qty := 0;
END IF;
l_used_plan_qty := l_available_plan_qty - l_remaining_plan_qty;
l_used_cons_plan_qty := l_used_cons_plan_qty + l_used_plan_qty;
--insert into forecast table
insert into BEC_DWH.FACT_ASCP_INSTALL_FORECAST_DETAILS 
values 
  (
	i_dates.forecast_designator, 
    i_dates.part_number, 
    i_dates.organization_id, 
    i_dates.forecast_date, 
    l_forecast_qty, 
    l_available_ohq, 
    l_available_po_qty, 
    l_available_plan_qty, 
    l_remaining_ohq, 
    l_remaining_po_qty, 
    l_remaining_plan_qty, 
    (
      l_available_ohq + l_available_po_qty + l_available_plan_qty
    ) - l_forecast_qty
  );
l_first_iteration := 'N';
END LOOP;
end $$;