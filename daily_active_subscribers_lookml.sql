view: active_paid_subscribers_daily {
  derived_table: {
    sql:
      with first_sub as (
        select
            user_id
          , min(starts_on) as first_sub_date
        from analytics.user_subscriptions
        where subscription_type not in ('education','cause')
        and order_id is not null
        group by 1),

      first_duration_and_type as (
      select
        s.user_id
      , case when s.plan_rate_duration = 'month' then 'monthly'
             when s.plan_rate_duration = 'year' then 'annual'
             else s.plan_rate_duration
        end as first_plan_duration
      , case when s.plan_display_name in ('Business','Business Builder') then 'Pro+' -- add case statement for Pro+ on 11/15/2021 by JB
             else s.plan_display_name
        end as first_plan_type
      , s.plan_category as first_plan_type_version
      , s.order_price_list_logical_id as first_sub_price_list
      from first_sub f
      join analytics.user_subscriptions s on s.user_id = f.user_id and s.starts_on = f.first_sub_date
      group by 1,2,3,4,5),

      set_not_to_renew_subs as (
      select
        r.user_id
      , s.id as subscription_id
      , min(r.cancelled_at) as sntr_date
      from web2.recurring_payments r
      join analytics.user_subscriptions s on r.user_id = s.user_id and r.cancelled_at between s.starts_on and s.ends_on
      where r.cancelled_at is not null
      group by 1,2)

      select d.date || s.subscription_id as table_key
      , d.date
      , s.subscription_id
      , lag(s.subscription_id,1) over (partition by s.user_id order by d.date, s.subscription_id) as previous_subscription_id
      , s.user_id
      , s.plan_type
      , s.plan_type_version
      , s.starts_on_date
      , s.ends_on_date
      , datediff(d,s.starts_on_date,s.ends_on_date) as length_of_sub
      , s.plan_duration
      , s.first_sub_date
      , s.first_sub_time
      , s.first_plan_duration
      , s.first_plan_type
      , s.first_plan_type_version
      , abs(floor(DATE_DIFF('w', d.date,s.first_sub_date))) as weeks_since_first_sub
      , floor(months_between(d.date,s.first_sub_date)) as months_since_first_sub
      , lag(s.plan_duration,1) over (partition by s.user_id order by d.date, s.subscription_id asc) as previous_plan_duration
      , lag(s.plan_type,1) over (partition by s.user_id order by d.date, s.subscription_id asc) as previous_plan_type
      , lag(s.plan_type_version,1) over (partition by s.user_id order by d.date, s.subscription_id asc) as previous_plan_type_version
      , lag(datediff(d,s.starts_on_date,s.ends_on_date),1) over (partition by s.user_id order by d.date, s.subscription_id asc) as previous_length_of_sub
      , datediff(d,lag(s.ends_on_date) over (partition by s.user_id order by d.date, s.subscription_id asc),d.date) as days_since_previous_sub
      , case when lead(d.date) over (partition by s.user_id order by d.date asc) is null and datediff(d,d.date,current_date) > 14
      then 1
      when datediff(d,d.date,lead(d.date) over (partition by s.user_id order by d.date asc)) > 14
      then 1
      else 0
      end as churned
      , s.payment_service
      , s.sntr_date
      , case when s.sntr_date is not null
      then 1
      else 0
      end as sub_sntr
      , s.price_list_logical_id
      , s.first_sub_price_list

      from util.days d

      left join (
      select
        s.id as subscription_id
      , s.user_id
      , s.user_plan_id
      , s.order_id
      , s.original_order_id
      , case when s.plan_display_name in ('Business','Business Builder')
      then 'Pro+'
      else s.plan_display_name
      end as plan_type
      , s.plan_category as plan_type_version
      , date(s.starts_on) as starts_on_date
      , date(dateadd(d,-1,s.ends_on)) as ends_on_date
      , case when s.plan_rate_duration = 'month' then 'monthly'
      when s.plan_rate_duration = 'year' then 'annual'
      else s.plan_rate_duration
      end as plan_duration
      , date(fs.first_sub_date) as first_sub_date
      , fs.first_sub_date as first_sub_time
      , fd.first_plan_duration
      , fd.first_plan_type
      , fd.first_plan_type_version
      , case when s.order_service_id = 1 then 'GoogleCheckout'
      when s.order_service_id = 2 then 'PayPal'
      when s.order_service_id = 3 then 'Braintree'
      when s.order_service_id = 4 then 'BraintreeBlue'
      when s.order_service_id = 5 then 'Apple'
      when s.order_service_id = 6 then 'Android'
      when s.order_service_id = 7 then 'PayPal_Braintree'
      when s.order_service_id = 8 then 'DR/Worldline'
      else 'Other'
      end as payment_service
      , sntr.sntr_date
      , s.order_price_list_logical_id as price_list_logical_id
      , fd.first_sub_price_list
      from analytics.user_subscriptions s
      left join first_sub fs on s.user_id = fs.user_id
      left join first_duration_and_type fd on s.user_id = fd.user_id
      left join set_not_to_renew_subs sntr on s.id = sntr.subscription_id
      where s.subscription_type not in ('education','cause') -- excludes non-paid subs
      and s.order_id is not null
      ) as s on d.date >= s.starts_on_date and d.date <= s.ends_on_date
      where d.date between '2014-01-01' and dateadd(d,365,getdate())
      ;;
    distribution: "date"
    sortkeys: ["date","user_id"]
    sql_trigger_value:  SELECT FLOOR((EXTRACT(epoch from GETDATE()) - 60*60*5.5)/(60*60*24)) ;;
  }

#### DIMENSIONS ####

  dimension: table_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.table_key ;;
  }

  dimension_group: subscription {
    type: time
    sql: ${TABLE}.date ;;
  }

  dimension: subscription_id {
    type: number
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: plan_type {
    type: string
    sql: ${TABLE}.plan_type ;;
  }

  dimension: plan_duration {
    description: "Monthly or annual"
    type: string
    sql: ${TABLE}.plan_duration ;;
  }

  dimension: plan_type_old {
    label: "Plan Type OLD"
    description: "This dimension uses the logic from the old model to determine the plan type."
    type: string
    sql: case when ${TABLE}.plan_type in ('Personal Builder','Professional Builder')
              then split_part(${TABLE}.plan_type, ' ', 1)
              when ${TABLE}.plan_type = 'Trial Builder'
              then 'Pro Trial'
              else coalesce(${TABLE}.plan_type, 'A La Carte')
              end
       ;;
  }

  ## Updated to change 'Team' to 'Pro+' on 11/15/2021 by JB
  dimension: main_plan_type {
    type: string
    sql: case when ${TABLE}.plan_type in ('Pro','Pro Premium','Professional','Professional Builder')
              then 'Professional'
              when ${TABLE}.plan_type in ('Personal','Personal Builder')
              then 'Personal'
              when ${TABLE}.plan_type = 'Pro+'
              then 'Pro+'
              else 'Other'
              end ;;
  }

  dimension: plan_type_version {
    label: "Plan Type & Version"
    type: string
    sql: ${TABLE}.plan_type_version ;;
  }

  dimension_group: starts_on {
    label: "Subscription Starts On"
    type: time
    sql: ${TABLE}.starts_on_date ;;
  }

  dimension_group: ends_on {
    label: "Subscription Ends On"
    type: time
    sql: ${TABLE}.ends_on_date ;;
  }

  dimension: length_of_sub {
    label: "Length of Subscription"
    description: "In days."
    type: number
    sql: ${TABLE}.length_of_sub ;;
  }

  dimension: weeks_since_first_sub {
    label: "Weeks Since First Subscription"
    type: number
    sql: ${TABLE}.weeks_since_first_sub ;;
    group_label: "First Subscription Dimensions"
  }

  dimension: months_since_first_sub {
    label: "Months Since First Subscription"
    type: number
    sql: ${TABLE}.months_since_first_sub ;;
    group_label: "First Subscription Dimensions"
  }

  dimension_group: first_subscription {
    type: time
    sql: ${TABLE}.first_sub_date ;;
    group_label: "First Subscription Dimensions"
  }

  dimension_group: first_sub_time {
    type: time
    hidden: yes
    sql: ${TABLE}.first_sub_time ;;
    group_label: "First Subscription Dimensions"
  }

  dimension: first_plan_duration {
    description: "Duration of first subscription plan: monthly or annual"
    type: string
    sql: ${TABLE}.first_plan_duration ;;
    group_label: "First Subscription Dimensions"
  }

  dimension: first_plan_type {
    type: string
    sql: ${TABLE}.first_plan_type ;;
    group_label: "First Subscription Dimensions"
  }

  dimension: first_plan_type_old {
    label: "First Plan Type OLD"
    type: string
    sql: case when ${TABLE}.first_plan_type in ('Personal Builder','Professional Builder')
              then split_part(${TABLE}.first_plan_type, ' ', 1)
              when ${TABLE}.first_plan_type = 'Trial Builder'
              then 'Pro Trial'
              else coalesce(${TABLE}.first_plan_type, 'A La Carte')
              end ;;
    group_label: "First Subscription Dimensions"
  }

  dimension: first_plan_type_version {
    label: "First Plan Type & Version"
    type: string
    sql: ${TABLE}.first_plan_type_version ;;
    group_label: "First Subscription Dimensions"
  }

  dimension: previous_plan_duration {
    description: "A value of 'monthly' or 'annual' for the previous paid plan for each user."
    type: string
    sql: ${TABLE}.previous_plan_duration ;;
  }

  dimension: previous_plan_type {
    description: "The plan type of the previous paid plan for each user."
    type: string
    sql: ${TABLE}.previous_plan_type ;;
  }

  dimension: previous_plan_type_version {
    label: "Previous Plan Type & Version"
    type: string
    sql: ${TABLE}.previous_plan_type_version ;;
  }

  dimension: previous_length_of_sub {
    description: "The number of days in length of the previous paid plan for each user."
    type: number
    sql: ${TABLE}.previous_length_of_sub ;;
  }

  dimension: days_since_previous_sub {
    description: "The number of days between the current subscription and the last subscription for each user."
    type: number
    sql: ${TABLE}.days_since_previous_sub ;;
  }

  dimension: upgrade_from_monthly_to_annual_plan {
    description: "Did the user upgrade from a monthly plan to an annual plan before churning? This does not include users who upgraded after churning."
    type: yesno
    sql: case when ${previous_plan_duration} = 'monthly' and ${plan_duration} = 'annual'
                and ${previous_length_of_sub} <= 32 and ${days_since_previous_sub} between 0 and 13
              then 1
              else 0
              end ;;
  }

  dimension: churned {
    label: "Did User Churn?"
    type: yesno
    sql: ${TABLE}.churned = 1 ;;
  }

  dimension: subscription_payment_service {
    type: string
    sql: ${TABLE}.payment_service ;;
  }

  dimension: plan_status_from_first_plan {
    label: "Plan Status From First Plan"
    description: "Indicates if the plan was an upgrade, downgrade, or the same from the user's very first plan. The tag is associated with every day of the active subscription."
    type: string
    sql: case when ${plan_type} in ('Pro+','Professional','Professional Builder','Pro','Pro Premium','Reseller') and ${first_plan_type} not in ('Pro+','Professional','Professional Builder','Pro','Pro Premium','Reseller')
              then 'upgrade'
              when ${plan_type} not in ('Pro+','Professional','Professional Builder','Pro','Pro Premium','Reseller') and ${first_plan_type} in ('Pro+','Professional','Professional Builder','Pro','Pro Premium','Reseller')
              then 'downgrade'
              else 'same'
              end ;;
  }

  dimension_group: set_not_to_renew {
    type: time
    sql: case when date(${TABLE}.sntr_date) > ${ends_on_date}
              then dateadd(d,-1,${TABLE}.sntr_date)
              else ${TABLE}.sntr_date
              end ;;
  }

  dimension: was_subscription_sntr {
    label: "Was Subscription SNTR?"
    description: "Was the subscription Set Not To Renew in the user's Account settings?"
    type: yesno
    sql: ${TABLE}.sub_sntr = 1 ;;
  }

  dimension: was_sub_not_sntr_yet {
    label: "Was Sub Not SNTR Yet?"
    description: "Was the date associated with the subscription BEFORE the user clicked SNTR?"
    type: yesno
    sql: ((${subscription_date} < ${set_not_to_renew_date}) and ${TABLE}.sub_sntr = 1) ;;
  }

  dimension: days_between_sub_start_and_sntr {
    label: "Days Btwn Sub Start & SNTR"
    description: "Counts the total number of days between the subscription start date and the set not to renew date."
    type: number
    sql: datediff(d,${starts_on_date},${set_not_to_renew_date}) ;;
  }

  dimension: days_between_sub_start_and_sntr_bucket {
    label: "Days Btwn Sub Start & SNTR (Bucket)"
    type: tier
    style: integer
    tiers: [0,1,7,14,31,180,365]
    sql: ${days_between_sub_start_and_sntr} ;;
  }

  dimension: sntr_on_this_day {
    label: "SNTR Renew On This Day?"
    description: "Value is 'Yes' if the paid user SNTR on this specific date."
    type: yesno
    sql: ${set_not_to_renew_date} = ${subscription_date} ;;
  }

  dimension: was_subscription_sntr_within_30d {
    label: "Was Subscription SNTR within 30D?"
    description: "Value is 'Yes' if the paid user SNTR within 30 days of the subscription starting."
    type: yesno
    sql: ${days_between_sub_start_and_sntr} <= 30 ;;
  }

  dimension: is_first_day_of_month {
    type: yesno
    sql: EXTRACT(day from ${subscription_date}) = 1 ;;
  }

  dimension: is_last_day_of_month {
    type: yesno
    sql: EXTRACT(day from DATEADD(day,1,${subscription_date})) = 1 ;;
  }

  dimension: price_list_of_subscription {
    description: "The price list ID associated with the order of the subscription. This should be used on looks with the Subscription Date dimensions."
    type: number
    sql: ${TABLE}.price_list_logical_id ;;
  }

  dimension: price_list_of_first_sub {
    description: "The price list ID associated with the order of the first subscription by the user. This should be used on looks with the First Subscription Date dimensions."
    type: number
    sql: ${TABLE}.first_sub_price_list ;;
    group_label: "First Subscription Dimensions"
  }

#### MEASURES ####

  measure: total_paid_subscribers {
    description: "Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: total_paid_subscribers_month_1 {
    description: "Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: ${user_id} ;;
    filters: [months_since_first_sub: "0"]
    group_label: "Monthly Paid Subscriber Measures"
  }

  measure: total_paid_subscribers_month_2 {
    description: "Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: ${user_id} ;;
    filters: [months_since_first_sub: "1"]
    group_label: "Monthly Paid Subscriber Measures"
  }

  measure: total_paid_subscribers_month_3 {
    description: "Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: ${user_id} ;;
    filters: [months_since_first_sub: "2"]
    group_label: "Monthly Paid Subscriber Measures"
  }

  measure: total_paid_subscribers_today {
    description: "Paid subscribers on the day of query. Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: case when ${subscription_date} = date(getdate()) then ${user_id} else null end ;;
  }

  measure: total_paid_subscribers_first_day_of_month {
    label: "Total Paid Subscribers (First Day of Month)"
    description: "Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: ${user_id} ;;
    filters: [is_first_day_of_month: "yes"]
  }

  measure: total_paid_subscribers_last_day_of_month {
    label: "Total Paid Subscribers (Last Day of Month)"
    description: "Does not include 'education' or 'cause' plan types, or users who were granted a subscription without paying."
    type: count_distinct
    sql: ${user_id} ;;
    filters: [is_last_day_of_month: "yes"]
  }

  measure: total_paid_professional_subscribers {
    description: "Total users who have paid a Pro, Professional, or Professional Builder subscription."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: plan_type
      value: "Pro, Professional, Professional Builder"
    }
  }

  measure: total_churned_subscribers {
    description: "Total users who did not renew their subscription within 14 days of it ending."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: churned
      value: "yes"
    }
  }

  measure: total_paid_users_with_1_plus_final {
    label: "Total Paid Users w/ 1+ Finalized Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Finalized Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_2_plus_final {
    label: "Total Paid Users w/ 2+ Finalized Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 2"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Finalized Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_3_plus_final {
    label: "Total Paid Users w/ 3+ Finalized Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 3"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Finalized Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_4_plus_final {
    label: "Total Paid Users w/ 4+ Finalized Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 4"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Finalized Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_5_plus_final {
    label: "Total Paid Users w/ 5+ Finalized Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 5"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Finalized Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_10_plus_final {
    label: "Total Paid Users w/ 10+ Finalized Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 10"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Finalized Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_1_plus_final_maker {
    label: "Total Paid Users w/ 1+ Finalized Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Finalized Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_2_plus_final_maker {
    label: "Total Paid Users w/ 2+ Finalized Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 2"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Finalized Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_3_plus_final_maker {
    label: "Total Paid Users w/ 3+ Finalized Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 3"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Finalized Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_4_plus_final_maker {
    label: "Total Paid Users w/ 4+ Finalized Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 4"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Finalized Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_5_plus_final_maker {
    label: "Total Paid Users w/ 5+ Finalized Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 5"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Finalized Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_10_plus_final_maker {
    label: "Total Paid Users w/ 10+ Finalized Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month.total_videos_finalized
      value: ">= 10"
    }
    filters: {
      field: projects_final_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Finalized Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_1_plus_final_combined {
    label: "Total Paid Users w/ 1+ Finalized Videos"
    description: "Builder & Maker projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month_rollup.total_videos_finalized
      value: ">= 1"
    }
    group_label: "Paid Users w/ Finalized Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_2_plus_final_combined {
    label: "Total Paid Users w/ 2+ Finalized Videos"
    description: "Builder & Maker projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month_rollup.total_videos_finalized
      value: ">= 2"
    }
    group_label: "Paid Users w/ Finalized Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_3_plus_final_combined {
    label: "Total Paid Users w/ 3+ Finalized Videos"
    description: "Builder & Maker projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month_rollup.total_videos_finalized
      value: ">= 3"
    }
    group_label: "Paid Users w/ Finalized Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_4_plus_final_combined {
    label: "Total Paid Users w/ 4+ Finalized Videos"
    description: "Builder & Maker projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month_rollup.total_videos_finalized
      value: ">= 4"
    }
    group_label: "Paid Users w/ Finalized Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_5_plus_final_combined {
    label: "Total Paid Users w/ 5+ Finalized Videos"
    description: "Builder & Maker projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month_rollup.total_videos_finalized
      value: ">= 5"
    }
    group_label: "Paid Users w/ Finalized Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_10_plus_final_combined {
    label: "Total Paid Users w/ 10+ Finalized Videos"
    description: "Builder & Maker projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_month_rollup.total_videos_finalized
      value: ">= 10"
    }
    group_label: "Paid Users w/ Finalized Videos"
    view_label: "Paid Subscribers"
  }

### CREATED VIDEO MEASURES ###

  measure: total_paid_users_with_1_plus_created {
    label: "Total Paid Users w/ 1+ Created Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 1"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Created Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_2_plus_created {
    label: "Total Paid Users w/ 2+ Created Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 2"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Created Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_3_plus_created {
    label: "Total Paid Users w/ 3+ Created Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 3"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Created Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_4_plus_created {
    label: "Total Paid Users w/ 4+ Created Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 4"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Created Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_5_plus_created {
    label: "Total Paid Users w/ 5+ Created Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 5"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Created Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_10_plus_created {
    label: "Total Paid Users w/ 10+ Created Builder Videos"
    description: "Builder projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 10"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "builder"
    }
    group_label: "Paid Users w/ Created Builder Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_1_plus_created_maker {
    label: "Total Paid Users w/ 1+ Created Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 1"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Created Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_2_plus_created_maker {
    label: "Total Paid Users w/ 2+ Created Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 2"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Created Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_3_plus_created_maker {
    label: "Total Paid Users w/ 3+ Created Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 3"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Created Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_4_plus_created_maker {
    label: "Total Paid Users w/ 4+ Created Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 4"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Created Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_5_plus_created_maker {
    label: "Total Paid Users w/ 5+ Created Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 5"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Created Maker Videos"
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_10_plus_created_maker {
    label: "Total Paid Users w/ 10+ Created Maker Videos"
    description: "Maker projects only."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_created_by_month.total_videos_created
      value: ">= 10"
    }
    filters: {
      field: projects_created_by_month.project_type
      value: "slideshow"
    }
    group_label: "Paid Users w/ Created Maker Videos"
    view_label: "Paid Subscribers"
  }

### SET NOT TO RENEW MEASURES ###

  measure: total_paid_users_who_sntr {
    label: "Total Paid Users Who SNTR"
    description: "The total numbers of users who cancelled their subscription by turning off automatic renewal."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    group_label: "SNTR Measures"
  }

  measure: total_paid_users_who_did_not_sntr {
    label: "Total Paid Users Who Did Not SNTR"
    description: "The total numbers of users who did not cancel their subscription by turning off automatic renewal."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: was_subscription_sntr
      value: "no"
    }
    group_label: "SNTR Measures"
  }

  measure: total_paid_users_who_sntr_within_30_days {
    label: "Total Paid Users Who SNTR w/in 30D"
    description: "The total numbers of users who cancelled their subscription by turning off automatic renewal within 30 days of the subscription start date."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    filters: {
      field: days_between_sub_start_and_sntr
      value: "<= 30"
    }
    group_label: "SNTR Measures"
  }

  measure: total_paid_users_who_sntr_within_7_days {
    label: "Total Paid Users Who SNTR w/in 7D"
    description: "The total numbers of users who cancelled their subscription by turning off automatic renewal within 7 days of the subscription start date."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    filters: {
      field: days_between_sub_start_and_sntr
      value: "<= 7"
    }
    group_label: "SNTR Measures"
  }

  measure: total_paid_users_who_sntr_within_14_days {
    label: "Total Paid Users Who SNTR w/in 14D"
    description: "The total numbers of users who cancelled their subscription by turning off automatic renewal within 14 days of the subscription start date."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    filters: {
      field: days_between_sub_start_and_sntr
      value: "<= 14"
    }
    group_label: "SNTR Measures"
  }

  measure: total_paid_users_who_sntr_within_24_hours {
    label: "Total Paid Users Who SNTR w/in 24H"
    description: "The total numbers of users who cancelled their subscription by turning off automatic renewal within 24 hours of the subscription start date."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    filters: {
      field: days_between_sub_start_and_sntr
      value: "<= 1"
    }
    group_label: "SNTR Measures"
  }

  measure: total_paid_users_sntr_in_period {
    label: "Total Paid Users Who SNTR in This Period"
    description: "The total numbers of users who cancelled their subscription by turning off automatic renewal within the period specified on this look."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: sntr_on_this_day
      value: "yes"
    }
    group_label: "SNTR Measures"
  }

  measure: total_sntr_users_with_finalized_video {
    label: "Total SNTR Users w/ 1+ Finalized Video"
    description: "Counts users who had a finalized video during a subscription that was set not to renew."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_day_rollup.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    group_label: "SNTR Measures"
  }

  measure: total_sntr_users_with_video_after_sntr {
    label: "Total SNTR Users w/ 1+ Finalized Video After SNTR"
    description: "Counts users who had a finalized video AFTER they set their subscription to not renew."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_day_rollup.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    filters: {
      field: was_sub_not_sntr_yet
      value: "no"
    }
    group_label: "SNTR Measures"
  }

  measure: total_non_sntr_users_with_finalized_video {
    label: "Total Non-SNTR Users w/ 1+ Finalized Video"
    description: "Counts users who had a finalized video during a subscription that was NOT set not to renew."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: projects_final_by_day_rollup.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: was_subscription_sntr
      value: "no"
    }
    group_label: "SNTR Measures"
  }

  measure: total_projects_finalized_by_sntr_users {
    label: "Total Projects Finalized by SNTR Users"
    description: "Counts projects that were finalized by users during a subscription that was set not to renew."
    type: sum
    sql: ${projects_final_by_day_rollup.total_videos_finalized} ;;
    filters: {
      field: projects_final_by_day_rollup.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    group_label: "SNTR Measures"
  }

  measure: total_projects_finalized_by_non_sntr_users {
    label: "Total Projects Finalized by Non-SNTR Users"
    description: "Counts projects that were finalized by users during a subscription that was NOT set not to renew."
    type: sum
    sql: ${projects_final_by_day_rollup.total_videos_finalized} ;;
    filters: {
      field: projects_final_by_day_rollup.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: was_subscription_sntr
      value: "no"
    }
    group_label: "SNTR Measures"
  }

  measure: total_projects_finalized_by_sntr_users_after_sntr {
    label: "Total Projects Finalized By SNTR Users After SNTR"
    description: "Counts projects that were finalized by users AFTER they set their subscription to not renew."
    type: sum
    sql: ${projects_final_by_day_rollup.total_videos_finalized} ;;
    filters: {
      field: projects_final_by_day_rollup.total_videos_finalized
      value: ">= 1"
    }
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    filters: {
      field: was_sub_not_sntr_yet
      value: "no"
    }
    group_label: "SNTR Measures"
  }

  measure: average_days_between_sub_start_and_sntr {
    label: "Average Days Between Sub Start & SNTR"
    type: average_distinct
    sql_distinct_key: ${subscription_id} ;;
    sql: ${days_between_sub_start_and_sntr} ;;
    filters: {
      field: was_subscription_sntr
      value: "yes"
    }
    value_format_name: decimal_1
    group_label: "SNTR Measures"
  }


### STOCK ASSET MEASURES ###

  measure: total_paid_users_with_stock_asset_project {
    label: "Total Paid Users w/ Stock Asset Project"
    description: "Only counts finalized projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: user_stock_assets_by_date.has_used_stock_assets
      value: "yes"
    }
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_stock_asset_builder_project {
    label: "Total Paid Users w/ Stock Asset Builder Project"
    description: "Only counts finalized projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: user_stock_assets_by_date.has_used_stock_assets_on_builder_project
      value: "yes"
    }
    view_label: "Paid Subscribers"
  }

  measure: total_paid_users_with_stock_asset_maker_project {
    label: "Total Paid Users w/ Stock Asset Maker Project"
    description: "Only counts finalized projects."
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: user_stock_assets_by_date.has_used_stock_assets_on_maker_project
      value: "yes"
    }
    view_label: "Paid Subscribers"
  }

  set: detail {
    fields: [
      table_key,
      subscription_date,
      subscription_day_of_month,
      subscription_day_of_year,
      subscription_week,
      subscription_week_of_year,
      subscription_month,
      subscription_month_num,
      subscription_quarter,
      subscription_quarter_of_year,
      subscription_year,
      subscription_id,
      user_id,
      days_since_previous_sub,
      plan_type,
      plan_type_old,
      plan_type_version,
      main_plan_type,
      plan_duration,
      first_plan_duration,
      first_plan_type,
      first_plan_type_version,
      first_plan_type_old,
      starts_on_date,
      starts_on_week,
      starts_on_month,
      starts_on_year,
      ends_on_date,
      ends_on_week,
      ends_on_month,
      ends_on_year,
      length_of_sub,
      weeks_since_first_sub,
      months_since_first_sub,
      first_subscription_date,
      first_subscription_week,
      first_subscription_month,
      first_subscription_month_num,
      first_subscription_year,
      previous_plan_duration,
      previous_plan_type,
      previous_plan_type_version,
      upgrade_from_monthly_to_annual_plan,
      total_paid_subscribers,
      total_paid_subscribers_today,
      total_churned_subscribers,
      churned,
      subscription_payment_service,
      plan_status_from_first_plan,
      set_not_to_renew_date,
      set_not_to_renew_week,
      set_not_to_renew_month,
      set_not_to_renew_month_num,
      set_not_to_renew_quarter,
      set_not_to_renew_quarter_of_year,
      set_not_to_renew_year,
      was_subscription_sntr,
      was_sub_not_sntr_yet,
      days_between_sub_start_and_sntr,
      days_between_sub_start_and_sntr_bucket,
      sntr_on_this_day,
      was_subscription_sntr_within_30d,
      is_last_day_of_month,
      is_first_day_of_month,
      price_list_of_subscription,
      price_list_of_first_sub,
      total_paid_subscribers_first_day_of_month,
      total_paid_subscribers_last_day_of_month,
      total_paid_users_with_1_plus_final,
      total_paid_users_with_2_plus_final,
      total_paid_users_with_3_plus_final,
      total_paid_users_with_4_plus_final,
      total_paid_users_with_5_plus_final,
      total_paid_users_with_10_plus_final,
      total_paid_users_with_1_plus_final_maker,
      total_paid_users_with_2_plus_final_maker,
      total_paid_users_with_3_plus_final_maker,
      total_paid_users_with_4_plus_final_maker,
      total_paid_users_with_5_plus_final_maker,
      total_paid_users_with_10_plus_final_maker,
      total_paid_users_with_1_plus_final_combined,
      total_paid_users_with_2_plus_final_combined,
      total_paid_users_with_3_plus_final_combined,
      total_paid_users_with_4_plus_final_combined,
      total_paid_users_with_5_plus_final_combined,
      total_paid_users_with_10_plus_final_combined,
      total_paid_users_who_sntr,
      total_paid_users_who_did_not_sntr,
      total_paid_users_with_stock_asset_project,
      total_paid_users_with_stock_asset_builder_project,
      total_paid_users_with_stock_asset_maker_project,
      total_paid_users_who_sntr_within_30_days,
      total_paid_users_who_sntr_within_7_days,
      total_paid_users_who_sntr_within_24_hours,
      total_paid_users_who_sntr_within_14_days,
      total_paid_users_sntr_in_period,
      total_sntr_users_with_finalized_video,
      total_sntr_users_with_video_after_sntr,
      total_non_sntr_users_with_finalized_video,
      total_projects_finalized_by_sntr_users,
      total_projects_finalized_by_non_sntr_users,
      total_projects_finalized_by_sntr_users_after_sntr,
      average_days_between_sub_start_and_sntr,
      total_paid_subscribers_month_1,
      total_paid_subscribers_month_2,
      total_paid_subscribers_month_3
    ]
  }

  set: projects_created {
    fields: [
      total_paid_users_with_1_plus_created,
      total_paid_users_with_2_plus_created,
      total_paid_users_with_3_plus_created,
      total_paid_users_with_4_plus_created,
      total_paid_users_with_5_plus_created,
      total_paid_users_with_10_plus_created,
      total_paid_users_with_1_plus_created_maker,
      total_paid_users_with_2_plus_created_maker,
      total_paid_users_with_3_plus_created_maker,
      total_paid_users_with_4_plus_created_maker,
      total_paid_users_with_5_plus_created_maker,
      total_paid_users_with_10_plus_created_maker,
      total_paid_subscribers_month_1,
      total_paid_subscribers_month_2,
      total_paid_subscribers_month_3
    ]
  }

  set: basic_detail {
    fields: [
      table_key,
      subscription_date,
      subscription_day_of_month,
      subscription_day_of_year,
      subscription_week,
      subscription_week_of_year,
      subscription_month,
      subscription_month_num,
      subscription_quarter,
      subscription_quarter_of_year,
      subscription_year,
      subscription_id,
      user_id,
      days_since_previous_sub,
      plan_type,
      plan_type_old,
      plan_type_version,
      main_plan_type,
      plan_duration,
      first_plan_duration,
      first_plan_type,
      first_plan_type_version,
      first_plan_type_old,
      starts_on_date,
      starts_on_week,
      starts_on_month,
      starts_on_year,
      ends_on_date,
      ends_on_week,
      ends_on_month,
      ends_on_year,
      length_of_sub,
      weeks_since_first_sub,
      months_since_first_sub,
      first_subscription_date,
      first_subscription_week,
      first_subscription_month,
      first_subscription_month_num,
      first_subscription_year,
      previous_plan_duration,
      previous_plan_type,
      previous_plan_type_version,
      upgrade_from_monthly_to_annual_plan,
      total_paid_subscribers,
      total_paid_subscribers_today,
      total_churned_subscribers,
      churned,
      subscription_payment_service,
      plan_status_from_first_plan,
      set_not_to_renew_date,
      set_not_to_renew_week,
      set_not_to_renew_month,
      set_not_to_renew_month_num,
      set_not_to_renew_quarter,
      set_not_to_renew_quarter_of_year,
      set_not_to_renew_year,
      was_subscription_sntr,
      was_sub_not_sntr_yet,
      days_between_sub_start_and_sntr,
      days_between_sub_start_and_sntr_bucket,
      sntr_on_this_day,
      was_subscription_sntr_within_30d,
      is_last_day_of_month,
      is_first_day_of_month,
      price_list_of_subscription,
      price_list_of_first_sub,
      total_paid_subscribers_first_day_of_month,
      total_paid_subscribers_last_day_of_month,
      total_paid_users_who_sntr,
      total_paid_users_who_did_not_sntr,
      total_paid_users_who_sntr_within_30_days,
      total_paid_users_who_sntr_within_7_days,
      total_paid_users_who_sntr_within_24_hours,
      total_paid_users_who_sntr_within_14_days,
      total_paid_users_sntr_in_period,
      average_days_between_sub_start_and_sntr,
      total_paid_subscribers_month_1,
      total_paid_subscribers_month_2,
      total_paid_subscribers_month_3
      ]
  }
}


