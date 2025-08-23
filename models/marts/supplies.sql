with

supplies as (

    select * from {{ ref('stg_supplies') }}

),

order_items as (

    select * from {{ ref('order_items') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

supply_performance as (

    select
        supplies.supply_id,
        supplies.supply_name,
        supplies.product_id,
        supplies.supply_cost,
        supplies.is_perishable_supply,

        count(distinct order_items.order_id) as orders_using_supply,
        count(order_items.order_item_id) as total_usage_quantity,
        sum(order_items.product_price) as revenue_generated,
        sum(order_items.supply_cost) as total_supply_cost_incurred,

        case
            when sum(order_items.supply_cost) > 0
            then sum(order_items.product_price) / sum(order_items.supply_cost)
            else null
        end as revenue_to_cost_ratio,

        avg(order_items.product_price) as avg_product_selling_price,

        case
            when count(order_items.order_item_id) > 0
            then sum(order_items.supply_cost) / count(order_items.order_item_id)
            else supplies.supply_cost
        end as avg_cost_per_usage

    from supplies

    left join order_items
        on supplies.product_id = order_items.product_id

    group by 1, 2, 3, 4, 5

),

supply_rankings as (

    select
        *,

        row_number() over (order by total_usage_quantity desc) as usage_rank,
        row_number() over (order by revenue_generated desc) as revenue_contribution_rank,
        row_number() over (order by revenue_to_cost_ratio desc) as efficiency_rank,
        row_number() over (order by total_supply_cost_incurred desc) as cost_rank

    from supply_performance

),

supplier_analysis as (

    select
        supplies.supply_name,

        count(distinct supplies.product_id) as products_supplied,
        avg(supplies.supply_cost) as avg_supply_cost,
        sum(supply_performance.total_usage_quantity) as total_supplier_usage,
        sum(supply_performance.revenue_generated) as total_supplier_revenue,

        case
            when sum(supply_performance.total_supply_cost_incurred) > 0
            then sum(supply_performance.revenue_generated) / sum(supply_performance.total_supply_cost_incurred)
            else null
        end as supplier_efficiency_ratio

    from supplies

    left join supply_performance
        on supplies.supply_id = supply_performance.supply_id

    group by 1

),

inventory_turnover as (

    select
        supplies.supply_id,
        supplies.supply_name,

        case
            when supplies.supply_cost > 0 and supply_performance.total_usage_quantity > 0
            then supply_performance.total_supply_cost_incurred / supplies.supply_cost
            else 0
        end as turnover_ratio,

        case
            when supply_performance.total_usage_quantity > 0
            then 365.0 / supply_performance.total_usage_quantity
            else null
        end as days_between_usage

    from supplies

    left join supply_performance
        on supplies.supply_id = supply_performance.supply_id

),

cost_variance_analysis as (

    select
        supplies.supply_id,

        supplies.supply_cost as standard_cost,
        supply_performance.avg_cost_per_usage as actual_avg_cost,

        case
            when supplies.supply_cost > 0
            then (supply_performance.avg_cost_per_usage - supplies.supply_cost) / supplies.supply_cost
            else 0
        end as cost_variance_percentage,

        abs(supply_performance.avg_cost_per_usage - supplies.supply_cost) as absolute_cost_variance

    from supplies

    left join supply_performance
        on supplies.supply_id = supply_performance.supply_id

),

final as (

    select
        supply_rankings.*,

        coalesce(inventory_turnover.turnover_ratio, 0) as inventory_turnover_ratio,
        coalesce(inventory_turnover.days_between_usage, 0) as avg_days_between_usage,

        coalesce(cost_variance_analysis.cost_variance_percentage, 0) as cost_variance_percentage,
        coalesce(cost_variance_analysis.absolute_cost_variance, 0) as absolute_cost_variance,

        case
            when usage_rank <= 5 then 'High Usage'
            when usage_rank <= 15 then 'Medium Usage'
            else 'Low Usage'
        end as usage_tier,

        case
            when efficiency_rank <= 5 then 'High Efficiency'
            when efficiency_rank <= 15 then 'Medium Efficiency'
            else 'Low Efficiency'
        end as efficiency_tier,

        case
            when is_perishable_supply and avg_days_between_usage > 7 then 'Risk: Slow Moving Perishable'
            when not is_perishable_supply and avg_days_between_usage > 30 then 'Risk: Slow Moving'
            when cost_variance_percentage > 0.2 then 'Risk: High Cost Variance'
            else 'Normal'
        end as risk_category

    from supply_rankings

    left join inventory_turnover
        on supply_rankings.supply_id = inventory_turnover.supply_id

    left join cost_variance_analysis
        on supply_rankings.supply_id = cost_variance_analysis.supply_id

)

select * from final
