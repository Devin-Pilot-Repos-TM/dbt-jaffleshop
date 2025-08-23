with

customers as (

    select * from {{ ref('customers') }}

),

orders as (

    select * from {{ ref('orders') }}

),

order_items as (

    select * from {{ ref('order_items') }}

),

rfm_analysis as (

    select
        customers.customer_id,
        customers.customer_name,
        customers.customer_type,
        customers.count_lifetime_orders,
        customers.lifetime_spend,
        customers.first_ordered_at,
        customers.last_ordered_at,

        {{ dbt.datediff('customers.last_ordered_at', 'current_date', 'day') }} as days_since_last_order,
        customers.count_lifetime_orders as frequency_score,
        customers.lifetime_spend as monetary_score,

        case
            when {{ dbt.datediff('customers.last_ordered_at', 'current_date', 'day') }} <= 30 then 5
            when {{ dbt.datediff('customers.last_ordered_at', 'current_date', 'day') }} <= 60 then 4
            when {{ dbt.datediff('customers.last_ordered_at', 'current_date', 'day') }} <= 90 then 3
            when {{ dbt.datediff('customers.last_ordered_at', 'current_date', 'day') }} <= 180 then 2
            else 1
        end as recency_score,

        case
            when customers.count_lifetime_orders >= 10 then 5
            when customers.count_lifetime_orders >= 7 then 4
            when customers.count_lifetime_orders >= 4 then 3
            when customers.count_lifetime_orders >= 2 then 2
            else 1
        end as frequency_quintile,

        case
            when customers.lifetime_spend >= 100 then 5
            when customers.lifetime_spend >= 75 then 4
            when customers.lifetime_spend >= 50 then 3
            when customers.lifetime_spend >= 25 then 2
            else 1
        end as monetary_quintile

    from customers

    where customers.customer_id is not null

),

rfm_segments as (

    select
        *,

        (recency_score + frequency_quintile + monetary_quintile) / 3.0 as rfm_score,

        case
            when recency_score >= 4 and frequency_quintile >= 4 and monetary_quintile >= 4 then 'Champions'
            when recency_score >= 3 and frequency_quintile >= 3 and monetary_quintile >= 3 then 'Loyal Customers'
            when recency_score >= 4 and frequency_quintile <= 2 and monetary_quintile >= 3 then 'Potential Loyalists'
            when recency_score >= 4 and frequency_quintile <= 2 and monetary_quintile <= 2 then 'New Customers'
            when recency_score >= 3 and frequency_quintile >= 2 and monetary_quintile <= 2 then 'Promising'
            when recency_score <= 2 and frequency_quintile >= 3 and monetary_quintile >= 3 then 'Need Attention'
            when recency_score <= 2 and frequency_quintile >= 2 and monetary_quintile >= 2 then 'About to Sleep'
            when recency_score <= 2 and frequency_quintile >= 4 and monetary_quintile <= 2 then 'At Risk'
            when recency_score <= 1 and frequency_quintile >= 4 and monetary_quintile >= 4 then 'Cannot Lose Them'
            when recency_score <= 2 and frequency_quintile <= 2 and monetary_quintile >= 3 then 'Hibernating'
            else 'Lost'
        end as rfm_segment

    from rfm_analysis

),

customer_lifetime_value as (

    select
        customer_id,

        case
            when lifetime_spend >= 150 then 'High Value'
            when lifetime_spend >= 75 then 'Medium Value'
            when lifetime_spend >= 25 then 'Low Value'
            else 'Minimal Value'
        end as clv_tier,

        case
            when count_lifetime_orders >= 8 then 'Very Frequent'
            when count_lifetime_orders >= 5 then 'Frequent'
            when count_lifetime_orders >= 3 then 'Occasional'
            else 'Rare'
        end as purchase_frequency_tier

    from rfm_analysis

),

behavioral_segments as (

    select
        orders.customer_id,

        sum(case when orders.is_food_order then 1 else 0 end) as food_orders,
        sum(case when orders.is_drink_order then 1 else 0 end) as drink_orders,
        count(distinct orders.location_id) as locations_visited,

        case
            when sum(case when orders.is_food_order then 1 else 0 end) > sum(case when orders.is_drink_order then 1 else 0 end) then 'Food Focused'
            when sum(case when orders.is_drink_order then 1 else 0 end) > sum(case when orders.is_food_order then 1 else 0 end) then 'Drink Focused'
            else 'Balanced'
        end as product_preference,

        case
            when count(distinct orders.location_id) >= 3 then 'Multi-Location'
            when count(distinct orders.location_id) = 2 then 'Two-Location'
            else 'Single-Location'
        end as location_behavior

    from orders

    group by 1

),

product_preferences as (

    select
        orders.customer_id,

        count(distinct order_items.product_id) as unique_products_purchased,
        mode() within group (order by order_items.product_name) as favorite_product,

        case
            when count(distinct order_items.product_id) >= 5 then 'Variety Seeker'
            when count(distinct order_items.product_id) >= 3 then 'Moderate Variety'
            else 'Consistent Buyer'
        end as variety_preference

    from order_items

    left join orders on order_items.order_id = orders.order_id

    group by 1

),

churn_prediction as (

    select
        customer_id,

        case
            when days_since_last_order > 180 then 'High Risk'
            when days_since_last_order > 90 then 'Medium Risk'
            when days_since_last_order > 60 then 'Low Risk'
            else 'Active'
        end as churn_risk,

        case
            when rfm_segment in ('Lost', 'Hibernating', 'Cannot Lose Them') then 'Immediate Action'
            when rfm_segment in ('At Risk', 'About to Sleep', 'Need Attention') then 'Monitor Closely'
            else 'Maintain'
        end as retention_action

    from rfm_segments

),

final as (

    select
        rfm_segments.*,

        coalesce(customer_lifetime_value.clv_tier, 'Unknown') as clv_tier,
        coalesce(customer_lifetime_value.purchase_frequency_tier, 'Unknown') as purchase_frequency_tier,

        coalesce(behavioral_segments.food_orders, 0) as food_orders,
        coalesce(behavioral_segments.drink_orders, 0) as drink_orders,
        coalesce(behavioral_segments.locations_visited, 0) as locations_visited,
        coalesce(behavioral_segments.product_preference, 'Unknown') as product_preference,
        coalesce(behavioral_segments.location_behavior, 'Unknown') as location_behavior,

        coalesce(product_preferences.unique_products_purchased, 0) as unique_products_purchased,
        coalesce(product_preferences.favorite_product, 'Unknown') as favorite_product,
        coalesce(product_preferences.variety_preference, 'Unknown') as variety_preference,

        coalesce(churn_prediction.churn_risk, 'Unknown') as churn_risk,
        coalesce(churn_prediction.retention_action, 'Unknown') as retention_action

    from rfm_segments

    left join customer_lifetime_value
        on rfm_segments.customer_id = customer_lifetime_value.customer_id

    left join behavioral_segments
        on rfm_segments.customer_id = behavioral_segments.customer_id

    left join product_preferences
        on rfm_segments.customer_id = product_preferences.customer_id

    left join churn_prediction
        on rfm_segments.customer_id = churn_prediction.customer_id

)

select * from final
