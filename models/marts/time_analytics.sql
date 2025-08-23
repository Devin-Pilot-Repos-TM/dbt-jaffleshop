with

orders as (

    select * from {{ ref('orders') }}

),

time_spine as (

    select * from {{ ref('metricflow_time_spine') }}

),

daily_summary as (

    select
        orders.ordered_at as order_date,

        count(distinct orders.order_id) as daily_orders,
        count(distinct orders.customer_id) as daily_unique_customers,
        sum(orders.order_total) as daily_revenue,
        sum(orders.order_cost) as daily_cost,
        sum(orders.order_total) - sum(orders.order_cost) as daily_profit,
        avg(orders.order_total) as daily_avg_order_value,

        count(distinct case when orders.customer_order_number = 1 then orders.customer_id end) as daily_new_customers,
        count(distinct case when orders.customer_order_number > 1 then orders.customer_id end) as daily_returning_customers,

        sum(case when orders.is_food_order then 1 else 0 end) as daily_food_orders,
        sum(case when orders.is_drink_order then 1 else 0 end) as daily_drink_orders

    from orders

    group by 1

),

weekly_summary as (

    select
        {{ dbt.date_trunc('week', 'orders.ordered_at') }} as order_week,

        count(distinct orders.order_id) as weekly_orders,
        count(distinct orders.customer_id) as weekly_unique_customers,
        sum(orders.order_total) as weekly_revenue,
        sum(orders.order_cost) as weekly_cost,
        sum(orders.order_total) - sum(orders.order_cost) as weekly_profit,
        avg(orders.order_total) as weekly_avg_order_value

    from orders

    group by 1

),

monthly_summary as (

    select
        {{ dbt.date_trunc('month', 'orders.ordered_at') }} as order_month,

        count(distinct orders.order_id) as monthly_orders,
        count(distinct orders.customer_id) as monthly_unique_customers,
        sum(orders.order_total) as monthly_revenue,
        sum(orders.order_cost) as monthly_cost,
        sum(orders.order_total) - sum(orders.order_cost) as monthly_profit,
        avg(orders.order_total) as monthly_avg_order_value,

        extract(year from orders.ordered_at) as order_year,
        extract(month from orders.ordered_at) as order_month_num,
        extract(quarter from orders.ordered_at) as order_quarter

    from orders

    group by 1, 8, 9, 10

),

cohort_analysis as (

    select
        {{ dbt.date_trunc('month', 'orders.ordered_at') }} as cohort_month,
        orders.customer_id,
        min(orders.ordered_at) as first_order_date,
        {{ dbt.date_trunc('month', 'min(orders.ordered_at)') }} as first_order_month

    from orders

    group by 1, 2

),

cohort_retention as (

    select
        first_order_month,
        cohort_month,

        {{ dbt.datediff('first_order_month', 'cohort_month', 'month') }} as period_number,

        count(distinct customer_id) as customers

    from cohort_analysis

    group by 1, 2, 3

),

seasonal_trends as (

    select
        extract(month from orders.ordered_at) as month_of_year,
        extract(quarter from orders.ordered_at) as quarter_of_year,
        extract(dayofweek from orders.ordered_at) as day_of_week,

        count(distinct orders.order_id) as seasonal_orders,
        sum(orders.order_total) as seasonal_revenue,
        avg(orders.order_total) as seasonal_avg_order_value

    from orders

    group by 1, 2, 3

),

growth_metrics as (

    select
        order_month,
        monthly_revenue,

        lag(monthly_revenue, 1) over (order by order_month) as prev_month_revenue,
        lag(monthly_orders, 1) over (order by order_month) as prev_month_orders,

        case
            when lag(monthly_revenue, 1) over (order by order_month) > 0
            then (monthly_revenue - lag(monthly_revenue, 1) over (order by order_month)) / lag(monthly_revenue, 1) over (order by order_month)
            else null
        end as revenue_growth_rate,

        case
            when lag(monthly_orders, 1) over (order by order_month) > 0
            then (monthly_orders - lag(monthly_orders, 1) over (order by order_month)) * 1.0 / lag(monthly_orders, 1) over (order by order_month)
            else null
        end as order_growth_rate

    from monthly_summary

),

time_spine_with_data as (

    select
        time_spine.date_day,

        coalesce(daily_summary.daily_orders, 0) as daily_orders,
        coalesce(daily_summary.daily_revenue, 0) as daily_revenue,
        coalesce(daily_summary.daily_profit, 0) as daily_profit,
        coalesce(daily_summary.daily_unique_customers, 0) as daily_unique_customers,
        coalesce(daily_summary.daily_avg_order_value, 0) as daily_avg_order_value,

        {{ dbt.date_trunc('week', 'time_spine.date_day') }} as week_start,
        {{ dbt.date_trunc('month', 'time_spine.date_day') }} as month_start,
        extract(year from time_spine.date_day) as year,
        extract(month from time_spine.date_day) as month,
        extract(quarter from time_spine.date_day) as quarter,
        extract(dayofweek from time_spine.date_day) as day_of_week

    from time_spine

    left join daily_summary
        on time_spine.date_day = daily_summary.order_date

    where time_spine.date_day >= '2018-01-01'
      and time_spine.date_day <= current_date

)

select * from time_spine_with_data
