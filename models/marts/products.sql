with

products as (

    select * from {{ ref('stg_products') }}

),

order_items as (

    select * from {{ ref('order_items') }}

),

orders as (

    select * from {{ ref('orders') }}

),

product_performance as (

    select
        products.product_id,
        products.product_name,
        products.product_type,
        products.product_description,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,

        count(distinct order_items.order_id) as total_orders,
        count(order_items.order_item_id) as total_quantity_sold,
        sum(order_items.product_price) as total_revenue,
        sum(order_items.supply_cost) as total_supply_cost,
        sum(order_items.product_price) - sum(order_items.supply_cost) as total_profit,

        case
            when sum(order_items.supply_cost) > 0
            then (sum(order_items.product_price) - sum(order_items.supply_cost)) / sum(order_items.supply_cost)
            else null
        end as profit_margin_ratio,

        avg(order_items.product_price) as avg_selling_price,
        avg(order_items.supply_cost) as avg_supply_cost

    from products

    left join order_items
        on products.product_id = order_items.product_id

    group by 1, 2, 3, 4, 5, 6, 7

),

product_rankings as (

    select
        *,

        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_quantity_sold desc) as popularity_rank,
        row_number() over (order by total_profit desc) as profit_rank,
        row_number() over (order by profit_margin_ratio desc) as margin_rank

    from product_performance

),

seasonal_analysis as (

    select
        order_items.product_id,

        extract(quarter from order_items.ordered_at) as order_quarter,
        extract(month from order_items.ordered_at) as order_month,

        count(order_items.order_item_id) as quarterly_quantity,
        sum(order_items.product_price) as quarterly_revenue

    from order_items

    group by 1, 2, 3

),

location_analysis as (

    select
        order_items.product_id,
        orders.location_id,

        count(order_items.order_item_id) as location_quantity,
        sum(order_items.product_price) as location_revenue

    from order_items

    left join orders
        on order_items.order_id = orders.order_id

    group by 1, 2

),

product_location_summary as (

    select
        product_id,

        count(distinct location_id) as locations_sold_at,
        max(location_revenue) as best_location_revenue,
        min(location_revenue) as worst_location_revenue

    from location_analysis

    group by 1

),

product_seasonal_summary as (

    select
        product_id,

        max(quarterly_revenue) as peak_quarter_revenue,
        min(quarterly_revenue) as low_quarter_revenue,
        avg(quarterly_revenue) as avg_quarterly_revenue,

        case
            when max(quarterly_revenue) > 0
            then (max(quarterly_revenue) - min(quarterly_revenue)) / max(quarterly_revenue)
            else 0
        end as seasonality_index

    from seasonal_analysis

    group by 1

),

final as (

    select
        product_rankings.*,

        coalesce(product_location_summary.locations_sold_at, 0) as locations_sold_at,
        coalesce(product_location_summary.best_location_revenue, 0) as best_location_revenue,
        coalesce(product_location_summary.worst_location_revenue, 0) as worst_location_revenue,

        coalesce(product_seasonal_summary.peak_quarter_revenue, 0) as peak_quarter_revenue,
        coalesce(product_seasonal_summary.low_quarter_revenue, 0) as low_quarter_revenue,
        coalesce(product_seasonal_summary.avg_quarterly_revenue, 0) as avg_quarterly_revenue,
        coalesce(product_seasonal_summary.seasonality_index, 0) as seasonality_index,

        case
            when revenue_rank <= 5 then 'Top Performer'
            when revenue_rank <= 10 then 'High Performer'
            when revenue_rank <= 20 then 'Medium Performer'
            else 'Low Performer'
        end as performance_tier

    from product_rankings

    left join product_location_summary
        on product_rankings.product_id = product_location_summary.product_id

    left join product_seasonal_summary
        on product_rankings.product_id = product_seasonal_summary.product_id

)

select * from final
