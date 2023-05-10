with first_sub as (
    select 
    customer_id
    , order_id as first_order_id
    , channel first_channel
    , sub_type first_sub_type 
    , country first_country
    , case when sub_type = 'One-Time' then 'One-Time'
        when recurring_installment_period in ('Semi-Annually', 'Semi-Annual') then 'Semi-Annually'
        when recurring_installment_period in ('Annually', 'Yearly') then 'Annually'
        else recurring_installment_period end as first_subscription_period
    , close_date first_close_date
    , dateadd(month, datediff(month, 0, close_date), 0) first_close_month
    , amount initial_amount
    from orders
    where is_countable_revenue = 1
    and is_first_sub = 1
    and close_date < getdate()
)

-- build historical actual ltvs which will inform predictive downstream
, actual_ltv as (
    select
    f.customer_id
    , f.first_order_id
    , f.first_channel
    , f.first_sub_type 
    , f.first_country
    , f.first_subscription_period
    , f.first_close_date
    , f.first_close_month
    , f.initial_amount
    , sum(d.amount) as twelve_month_ltv
    , sum(case when datediff(month, f.first_close_date, close_date) < 6 then d.amount
        else null end) as six_month_ltv
    from first_sub f
    left join orders d on d.customer_id = f.customer_id
        and d.close_date >= f.first_close_date 
        and datediff(month, f.first_close_date, close_date) < 12
    group by 
    f.customer_id
    , f.first_order_id
    , f.first_channel
    , f.first_sub_type 
    , f.first_country
    , f.first_subscription_period
    , f.first_close_date
    , f.first_close_month
    , f.initial_donation_amount
)

, outlier_data as (
    select 
    stdev(twelve_month_ltv) as standard_deviation_twelve
    , avg(twelve_month_ltv) as average_twelve
    , stdev(six_month_ltv) as standard_deviation_six
    , avg(six_month_ltv) as average_six
    , stdev(initial_donation_amount) as standard_deviation_initial_amount
    , avg(initial_donation_amount) as average_initial_amount
    from actual_ltv
)

--identify outliers
, all_data_with_outlier_enrichment as (
    select
    customer_id
    , first_order_id
    , first_channel
    , first_sub_type 
    , first_country
    , first_subscription_period
    , first_close_date
    , first_close_month
    , initial_amount
    , twelve_month_ltv
    , six_month_ltv
    , case when twelve_month_ltv > (average_twelve + (3*standard_deviation_twelve)) then 1
        when twelve_month_ltv < (average_twelve - (3*standard_deviation_twelve)) then 1
        when six_month_ltv > (average_six + (3*standard_deviation_six)) then 1
        when six_month_ltv < (average_six - (3*standard_deviation_six)) then 1
        when initial_amount > (average_initial_amount + (3*standard_deviation_initial_amount)) then 1
        when initial_donation_amount < (average_initial_amount - (3*standard_deviation_initial_amount)) then 1
        else 0 end as is_outlier
    from actual_ltv 
    cross join outlier_data
)

-- remove outliers
, clean_actual as (
    select 
    customer_id
    , first_order_id
    , first_channel
    , first_sub_type 
    , first_country
    , first_subscription_period
    , first_close_date
    , first_close_month
    , initial_amount
    , twelve_month_ltv
    , six_month_ltv
    from all_data_with_outlier_enrichment
    where is_outlier = 0
    --removes 865 customers/records
)

, months as (
    select
    first_close_month as 'month' 
    from clean_actual
    where first_close_month >= '2018-10-01'
    group by first_close_month
)

-- normalize data across time 
, normalized_averages_by_month as (
    select
    m.month
    , c.first_channel
    , c.first_sub_type 
    , c.first_country
    , c.first_subscription_period
    , avg(case when c.first_close_month <= DATEADD(month, -6, m.month) and c.first_close_month  > DATEADD(month, -18, m.month)
            then c.initial_amount else null end) as trailing_avg_six_month_initial_amount
    , avg(case when c.first_close_month <= DATEADD(month, -12, m.month) and c.first_close_month  > DATEADD(month, -24, m.month)
            then c.initial_amount else null end) as trailing_avg_twelve_month_initial_amount
    , avg(case when c.first_close_month <= DATEADD(month, -6, m.month) and c.first_close_month  > DATEADD(month, -18, m.month)
            then c.six_month_ltv else null end) as trailing_avg_six_month_ltv
    , avg(case when c.first_close_month <= DATEADD(month, -12, m.month) and c.first_close_month  > DATEADD(month, -24, m.month)
            then c.twelve_month_ltv else null end) as trailing_avg_twelve_month_ltv
    from months as m
    left join clean_actual c on c.first_close_month <= DATEADD(month, -6, m.month)
        and c.first_close_month >= DATEADD(month, -18, m.month)
    group by 
    m.month
    , c.first_channel
    , c.first_sub_type 
    , c.first_country
    , c.first_subscription_period
)

-- build actual observed and predictive ltv for comparisons / validation of concept
select
a.customer_id
, first_order_id
, a.first_channel
, a.first_sub_type 
, a.first_country
, a.first_subscription_period
, a.first_close_date
, a.first_close_month
, a.initial_amount
, case when datediff(month, a.first_close_month, dateadd(month, datediff(month, 0, getdate()), 0) ) > 12 then a.twelve_month_ltv
    else null end as actual_twelve_month_ltv
, case when datediff(month, a.first_close_month, dateadd(month, datediff(month, 0, getdate()), 0) ) > 6 then a.twelve_month_ltv
    else null end as actual_six_month_ltv
, a.is_outlier
, case when a.initial_amount >=
    ((n.trailing_avg_six_month_ltv / n.trailing_avg_six_month_initial_amount) * a.initial_amount)
    then a.initial_amount 
    else ((n.trailing_avg_six_month_ltv / n.trailing_avg_six_month_initial_amount) * a.initial_amount)
    end as predictive_six_month_ltv
, case when a.initial_amount >=
    ((n.trailing_avg_twelve_month_ltv / n.trailing_avg_twelve_month_initial_amount) * a.initial_amount)
    then a.initial_amount 
    else ((n.trailing_avg_twelve_month_ltv / n.trailing_avg_twelve_month_initial_amount) * a.initial_amount)
    end as predictive_twelve_month_ltv
from all_data_with_outlier_enrichment a
left join normalized_averages_by_month n on a.first_close_month = n.month
    and a.first_channel = n.first_channel
    and a.first_sub_type = n.first_sub_type 
    and a.first_country = n.first_country
    and a.first_subscription_period = n.first_subscription_period
where a.first_close_month >= '2019-10-01'
