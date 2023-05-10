view: upsell_order_events {
  derived_table: {
    sql:
    --This view is intended to create an in-platform path which led to an order 
    --Being able to see user web behavior is a limitation as we only see 75-80% of users' behavior
      with events_en_route_to_order as (
        select
            *
          , case -- will be used to avoid ramdomly choosing ordering of events having the same timestamp
              when upsell_name = 'pricing' then 1
              when upsell_name = 'purchase' then 2
              when upsell_name = 'order' then 3
              else 0
            end as events_precedence
        from analytics.checkout_journey_events
        where en_route_to_order_id is not null
        -- considers only events within 60m prior to placing the order
        and event_tstamp + interval '60 minutes' >= en_route_to_order_placed_at
        -- removes seen events as they are not really interactions
        -- removes close events as there's always a previous open event for each close
        and user_action <> 'seen' and user_action <> 'close'
        -- removes export-modal events as they are considered 'seen' events
        and upsell_name <> 'export-modal'
      ),

      orders_path as (
        select
          en_route_to_order_id as order_id
        , listagg(upsell_name  || '/' || user_action|| ' at ' || origin, ' --> ') within group (order by event_tstamp, events_precedence) as whole_upsell_path
        , count(case when upsell_name not in ('order', 'purchase', 'pricing') then 1 end) as count_of_interactions_en_route_to_pricing
        from events_en_route_to_order
        group by 1
      ),

      orders_upsell_count as (
        select
          en_route_to_order_id as order_id
        , count(distinct case when upsell_name not in ('order', 'purchase', 'pricing') then upsell_name||upsell_type||origin end) as count_of_upsells_en_route_to_pricing
        from events_en_route_to_order
        group by 1
      ),

      -- get last upsell event before the order was placed, for those who interacted with an upsell
      upsell_event_id_per_order as (
        select
           en_route_to_order_id order_id
          , journey_type
          , upsell_event_id
          , upsell_event_tstamp
        from (
          select
            *
          , 'from-upsell' as journey_type
          , last_value(event_id) over (partition by en_route_to_order_id order by event_tstamp rows between unbounded preceding and unbounded following) upsell_event_id
          , last_value(event_tstamp) over (partition by en_route_to_order_id order by event_tstamp rows between unbounded preceding and unbounded following) upsell_event_tstamp
          from events_en_route_to_order
          where upsell_name not in ('order', 'purchase', 'pricing')
          and en_route_to_order_id in (
            select order_id
            from orders_path
            where count_of_interactions_en_route_to_pricing > 0
          )
        )
        group by 1,2,3,4
      ),

      -- get first pricing/purchase/order event before the order was placed, for those who did not interact with an upsell
      pricing_event_id_per_order as (
        select
           en_route_to_order_id order_id
          , journey_type
          , upsell_event_id
          , upsell_event_tstamp
        from (
          select
            *
          , 'not-from-upsell' as journey_type
          , first_value(event_id) over (partition by en_route_to_order_id order by event_tstamp, events_precedence rows between unbounded preceding and unbounded following) upsell_event_id
          , first_value(event_tstamp) over (partition by en_route_to_order_id order by event_tstamp, events_precedence rows between unbounded preceding and unbounded following) upsell_event_tstamp
          from events_en_route_to_order
          where en_route_to_order_id in (
            select order_id
            from orders_path
            where count_of_interactions_en_route_to_pricing = 0
          )
        )
        group by 1,2,3,4
      )

      select
          ev.effective_user_id as user_id
        , ev.en_route_to_order_id as order_id
        , ev.en_route_to_order_item_code as order_item_code
        , ev.en_route_to_order_placed_at as order_placed_at
        , orders_path.whole_upsell_path as order_whole_upsell_path
        , orders_path.count_of_interactions_en_route_to_pricing as order_count_of_interactions_en_route_to_pricing
        , orders_upsell_count.count_of_upsells_en_route_to_pricing as order_count_of_upsells_en_route_to_pricing
        , ev.event_id as upsell_event_id
        , ev.event_tstamp as upsell_event_tstamp
        , ev.event_platform as upsell_event_platform
        , ev.event_source as upsell_event_source
        , case
            when upsell_per_order.journey_type = 'not-from-upsell'
            then 'direct-to-' || upsell_name
            else ev.upsell_name
          end as upsell_name
        , ev.upsell_type
        , ev.user_action as upsell_user_action
        , ev.element_name as upsell_element_name
        , ev.origin as upsell_origin
        , ev.detail as upsell_detail
        , ev.project_okey
        , pl.id as plan_id_at_time_of_upsell_event
        , pl.display_name as plan_at_time_of_upsell_event
        , h.is_sub as is_paid_plan_at_time_of_upsell_event
      from events_en_route_to_order ev
      left join analytics.user_plans_history h on ev.effective_user_id = h.user_id
        and ev.event_tstamp >= h.starts_at
        and ev.event_tstamp < coalesce(h.ends_at_fixed,getdate())
      left join web2.plans pl on h.plan_id = pl.id
      join orders_path on ev.en_route_to_order_id = orders_path.order_id
      join orders_upsell_count on ev.en_route_to_order_id = orders_upsell_count.order_id
      join (
        select * from upsell_event_id_per_order
        union all
        select * from pricing_event_id_per_order
      ) upsell_per_order on ev.event_id = upsell_per_order.upsell_event_id and ev.event_tstamp = upsell_per_order.upsell_event_tstamp
 ;;
    distribution: "user_id"
    sortkeys: ["order_placed_at", "user_id"]
    sql_trigger_value:  SELECT COUNT(*) FROM ${checkout_journey_events.SQL_TABLE_NAME} ;;
  }

### DIMENSIONS ###

  dimension: upsell_event_id {
    hidden: yes
    type: string
    sql: ${TABLE}.upsell_event_id ;;
  }

  dimension: project_okey {
    type: string
    sql: ${TABLE}.project_okey ;;
  }

  dimension_group: upsell_event_timestamp {
    type: time
    sql: ${TABLE}.upsell_event_tstamp ;;
  }

  dimension: order_id {
    hidden: yes
    primary_key: yes
    type: number
    sql: ${TABLE}.order_id ;;
  }

  dimension: order_placed_at {
    hidden: yes
    type: date_time
    sql: ${TABLE}.order_placed_at ;;
  }

  dimension: user_id {
    hidden: yes
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: order_whole_upsell_path {
    type: string
    sql: ${TABLE}.order_whole_upsell_path ;;
  }

  dimension: upsell_name {
    type: string
    sql: ${TABLE}.upsell_name ;;
  }

  dimension: upsell_type {
    type: string
    sql: case when ${TABLE}.upsell_name = 'direct-to-pricing' and ${TABLE}.upsell_origin = 'pricing-page' and ${TABLE}.upsell_type = 'page'
              then 'pricing-page'
              else ${TABLE}.upsell_type end;;
  }

  dimension: upsell_user_action {
    type: string
    sql: ${TABLE}.upsell_user_action ;;
  }

  dimension: upsell_element_name {
    type: string
    sql: ${TABLE}.upsell_element_name ;;
  }

  dimension: upsell_origin {
    type: string
    sql: ${TABLE}.upsell_origin ;;
  }

  dimension: upsell_detail {
    type: string
    sql: ${TABLE}.upsell_detail ;;
  }

  dimension: upsell_count_en_route_to_pricing {
    description: "This is number of upsell interactions the user had prior to pricing or checkout."
    type: number
    sql: ${TABLE}.order_count_of_interactions_en_route_to_pricing ;;
  }

  dimension: distinct_upsell_count_en_route_to_pricing {
    description: "This is number of unique upsells the user had an interaction with prior to pricing or checkout."
    type: number
    sql: ${TABLE}.order_count_of_upsells_en_route_to_pricing ;;
  }

  dimension: plan_at_time_of_upsell_event {
    type: string
    sql: case when ${TABLE}.plan_at_time_of_upsell_event in ('Business','Business Builder')
              then 'Pro+'
              when ${TABLE}.plan_at_time_of_upsell_event is null
              then 'Unknown or Logged Out'
              else ${TABLE}.plan_at_time_of_upsell_event
              end ;;
  }

  dimension: plan_id_at_time_of_upsell_event {
    type: string
    sql: case when ${TABLE}.plan_id_at_time_of_upsell_event is null
              then 'Unknown or Logged Out'
              else ${TABLE}.plan_id_at_time_of_upsell_event
              end;;
  }

  dimension: paid_status_at_time_of_upsell_event {
    type: string
    sql: case when ${TABLE}.is_paid_plan_at_time_of_upsell_event is null
              then 'Unknown or Logged Out'
              when ${TABLE}.is_paid_plan_at_time_of_upsell_event = 1
              then 'Paid'
              else 'Free'
              end;;
  }

  dimension: is_new_user {
    label: "Is New User (7 Days)?"
    description: "Is the user within 7 days of registration?"
    type: yesno
    sql: datediff(days,${users_jb.created_date}, ${upsell_event_timestamp_date}) < 8;;
  }

### MEASURES ###

  measure: total_orders_from_upsells {
    type: count
  }

  measure: average_upsell_count {
    description: "This is average number of upsell interactions users had prior to pricing or checkout."
    type: average
    sql:  ${upsell_count_en_route_to_pricing}*1.0 ;;
    value_format_name: decimal_1
  }

  measure: average_distinct_upsell_count {
    description: "This is average number of unique upsells the user had an interaction with prior to pricing or checkout."
    type: average
    sql:  ${distinct_upsell_count_en_route_to_pricing}*1.0 ;;
    value_format_name: decimal_1
  }

}
