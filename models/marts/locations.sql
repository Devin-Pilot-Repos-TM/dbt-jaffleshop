with

locations as (

    select * from {{ ref('stg_locations') }}

),

orders as (

    select * from {{ ref('orders') }}

),

customers as (

    select * from {{ ref('customers') }}

),

location_performance as (

    select
        locations.location_id,
        locations.location_name,
        locations.tax_rate,
        locations.opened_date,

        count(distinct orders.order_id) as total_orders,
        count(distinct orders.customer_id) as unique_customers,
        sum(orders.order_total) as total_revenue,
        sum(orders.tax_paid) as total_tax_collected,
        sum(orders.order_cost) as total_supply_cost,
        sum(orders.order_total) - sum(orders.order_cost) as total_profit,

        avg(orders.order_total) as avg_order_value,
        avg(orders.order_cost) as avg_order_cost,

        case
            when sum(orders.order_cost) > 0
            then (sum(orders.order_total) - sum(orders.order_cost)) / sum(orders.order_cost)
            else null
        end as profit_margin_ratio,

        sum(case when orders.is_food_order then 1 else 0 end) as food_orders,
        sum(case when orders.is_drink_order then 1 else 0 end) as drink_orders,

        count(distinct case when orders.customer_order_number = 1 then orders.customer_id end) as new_customers,
        count(distinct case when orders.customer_order_number > 1 then orders.customer_id end) as returning_customers

    from locations

    left join orders
        on locations.location_id = orders.location_id

    group by 1, 2, 3, 4

),

location_rankings as (

    select
        *,

        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_orders desc) as order_volume_rank,
        row_number() over (order by unique_customers desc) as customer_base_rank,
        row_number() over (order by avg_order_value desc) as aov_rank,
        row_number() over (order by profit_margin_ratio desc) as profitability_rank

    from location_performance

),

customer_behavior_analysis as (

    select
        orders.location_id,

        avg(customers.count_lifetime_orders) as avg_customer_lifetime_orders,
        avg(customers.lifetime_spend) as avg_customer_lifetime_value,

        count(distinct case when customers.customer_type = 'new' then customers.customer_id end) as new_customer_count,
        count(distinct case when customers.customer_type = 'returning' then customers.customer_id end) as returning_customer_count,

        case
            when count(distinct customers.customer_id) > 0
            then count(distinct case when customers.customer_type = 'returning' then customers.customer_id end) * 1.0 / count(distinct customers.customer_id)
            else 0
        end as customer_retention_rate

    from orders

    left join customers
        on orders.customer_id = customers.customer_id

    group by 1

),

supply_chain_efficiency as (

    select
        orders.location_id,

        avg(orders.order_total - orders.order_cost) as avg_profit_per_order,
        stddev(orders.order_total - orders.order_cost) as profit_variance,

        case
            when avg(orders.order_total - orders.order_cost) > 0
            then stddev(orders.order_total - orders.order_cost) / avg(orders.order_total - orders.order_cost)
            else null
        end as profit_coefficient_of_variation

    from orders

    group by 1

),

final as (

    select
        location_rankings.*,

        coalesce(customer_behavior_analysis.avg_customer_lifetime_orders, 0) as avg_customer_lifetime_orders,
        coalesce(customer_behavior_analysis.avg_customer_lifetime_value, 0) as avg_customer_lifetime_value,
        coalesce(customer_behavior_analysis.customer_retention_rate, 0) as customer_retention_rate,

        coalesce(supply_chain_efficiency.avg_profit_per_order, 0) as avg_profit_per_order,
        coalesce(supply_chain_efficiency.profit_variance, 0) as profit_variance,
        coalesce(supply_chain_efficiency.profit_coefficient_of_variation, 0) as profit_coefficient_of_variation,

        case
            when revenue_rank <= 2 then 'Top Performer'
            when revenue_rank <= 4 then 'High Performer'
            else 'Standard Performer'
        end as performance_tier,

        case
            when customer_retention_rate >= 0.7 then 'High Retention'
            when customer_retention_rate >= 0.5 then 'Medium Retention'
            else 'Low Retention'
        end as retention_tier

    from location_rankings

    left join customer_behavior_analysis
        on location_rankings.location_id = customer_behavior_analysis.location_id

    left join supply_chain_efficiency
        on location_rankings.location_id = supply_chain_efficiency.location_id

)

select * from final
