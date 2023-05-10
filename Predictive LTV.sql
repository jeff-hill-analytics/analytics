with first_gifts as (
    select 
    account_id
    , opportunity_id as first_opportunity_id
    , mm_channel first_mm_channel
    , mm_gift_type first_mm_gift_type 
    , pillar first_pillar
    , case when mm_gift_type = 'One-Time' then 'One-Time'
        when recurring_installment_period in ('Semi-Annually', 'Semi-Annual') then 'Semi-Annually'
        when recurring_installment_period in ('Annually', 'Yearly') then 'Annually'
        else recurring_installment_period end as first_recurring_installment_period
    , close_date first_close_date
    , dateadd(month, datediff(month, 0, close_date), 0) first_close_month
    , opportunity_sf_usd_rav_amount initial_donation_amount
    from core.donations
    where is_mm_countable_revenue = 1
    and is_first_gift = 1
    and close_date < getdate()
)

, actual_ltv as (
    select
    f.account_id
    , f.first_opportunity_id
    , f.first_mm_channel
    , f.first_mm_gift_type 
    , f.first_pillar
    , f.first_recurring_installment_period
    , f.first_close_date
    , f.first_close_month
    , f.initial_donation_amount
    , sum(d.opportunity_sf_usd_rav_amount) as twelve_month_ltv
    , sum(case when datediff(month, f.first_close_date, close_date) < 6 then d.opportunity_sf_usd_rav_amount
        else null end) as six_month_ltv
    from first_gifts f
    left join core.donations d on d.account_id = f.account_id
        and d.close_date >= f.first_close_date 
        and datediff(month, f.first_close_date, close_date) < 12
    group by 
    f.account_id
    , f.first_opportunity_id
    , f.first_mm_channel
    , f.first_mm_gift_type 
    , f.first_pillar
    , f.first_recurring_installment_period
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
    , stdev(initial_donation_amount) as standard_deviation_initial_donation_amount
    , avg(initial_donation_amount) as average_initial_donation_amount
    from actual_ltv
)

--identify outliers
, all_data_with_outlier_enrichment as (
    select
    account_id
    , first_opportunity_id
    , first_mm_channel
    , first_mm_gift_type 
    , first_pillar
    , first_recurring_installment_period
    , first_close_date
    , first_close_month
    , initial_donation_amount
    , twelve_month_ltv
    , six_month_ltv
    , case when twelve_month_ltv > (average_twelve + (3*standard_deviation_twelve)) then 1
        when twelve_month_ltv < (average_twelve - (3*standard_deviation_twelve)) then 1
        when six_month_ltv > (average_six + (3*standard_deviation_six)) then 1
        when six_month_ltv < (average_six - (3*standard_deviation_six)) then 1
        when initial_donation_amount > (average_initial_donation_amount + (3*standard_deviation_initial_donation_amount)) then 1
        when initial_donation_amount < (average_initial_donation_amount - (3*standard_deviation_initial_donation_amount)) then 1
        else 0 end as is_outlier
    from actual_ltv 
    cross join outlier_data
)

, clean_actual as (
    select 
    account_id
    , first_opportunity_id
    , first_mm_channel
    , first_mm_gift_type 
    , first_pillar
    , first_recurring_installment_period
    , first_close_date
    , first_close_month
    , initial_donation_amount
    , twelve_month_ltv
    , six_month_ltv
    from all_data_with_outlier_enrichment
    where is_outlier = 0
    --removes 865 accounts/records
)

, months as (
    select
    first_close_month as 'month' 
    from clean_actual
    where first_close_month >= '2018-10-01'
    group by first_close_month
)

, normalized_averages_by_month as (
    select
    m.month
    , c.first_mm_channel
    , c.first_mm_gift_type 
    , c.first_pillar
    , c.first_recurring_installment_period
    , avg(case when c.first_close_month <= DATEADD(month, -6, m.month) and c.first_close_month  > DATEADD(month, -18, m.month)
            then c.initial_donation_amount else null end) as trailing_avg_six_month_initial_donation_amount
    , avg(case when c.first_close_month <= DATEADD(month, -12, m.month) and c.first_close_month  > DATEADD(month, -24, m.month)
            then c.initial_donation_amount else null end) as trailing_avg_twelve_month_initial_donation_amount
    , avg(case when c.first_close_month <= DATEADD(month, -6, m.month) and c.first_close_month  > DATEADD(month, -18, m.month)
            then c.six_month_ltv else null end) as trailing_avg_six_month_ltv
    , avg(case when c.first_close_month <= DATEADD(month, -12, m.month) and c.first_close_month  > DATEADD(month, -24, m.month)
            then c.twelve_month_ltv else null end) as trailing_avg_twelve_month_ltv
    from months as m
    left join clean_actual c on c.first_close_month <= DATEADD(month, -6, m.month)
        and c.first_close_month >= DATEADD(month, -18, m.month)
    group by 
    m.month
    , c.first_mm_channel
    , c.first_mm_gift_type 
    , c.first_pillar
    , c.first_recurring_installment_period
)

select
a.account_id
, first_opportunity_id
, a.first_mm_channel
, a.first_mm_gift_type 
, a.first_pillar
, a.first_recurring_installment_period
, a.first_close_date
, a.first_close_month
, a.initial_donation_amount
, case when datediff(month, a.first_close_month, dateadd(month, datediff(month, 0, getdate()), 0) ) > 12 then a.twelve_month_ltv
    else null end as actual_twelve_month_ltv
, case when datediff(month, a.first_close_month, dateadd(month, datediff(month, 0, getdate()), 0) ) > 6 then a.twelve_month_ltv
    else null end as actual_six_month_ltv
, a.is_outlier
, case when a.initial_donation_amount >=
    ((n.trailing_avg_six_month_ltv / n.trailing_avg_six_month_initial_donation_amount) * a.initial_donation_amount)
    then a.initial_donation_amount 
    else ((n.trailing_avg_six_month_ltv / n.trailing_avg_six_month_initial_donation_amount) * a.initial_donation_amount)
    end as predictive_six_month_ltv
, case when a.initial_donation_amount >=
    ((n.trailing_avg_twelve_month_ltv / n.trailing_avg_twelve_month_initial_donation_amount) * a.initial_donation_amount)
    then a.initial_donation_amount 
    else ((n.trailing_avg_twelve_month_ltv / n.trailing_avg_twelve_month_initial_donation_amount) * a.initial_donation_amount)
    end as predictive_twelve_month_ltv
from all_data_with_outlier_enrichment a
left join normalized_averages_by_month n on a.first_close_month = n.month
    and a.first_mm_channel = n.first_mm_channel
    and a.first_mm_gift_type = n.first_mm_gift_type 
    and a.first_pillar = n.first_pillar
where a.first_close_month >= '2019-10-01'