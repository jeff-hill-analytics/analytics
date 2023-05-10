view: launchdarkly_data_export {
  derived_table: {
    sql:
      -- create first date_time user interacted with test
      with user_feature_data as (
        select coalesce(u.id::text,ld.user_id) as effective_user_id
          , ld.feature as feature_name
          , ld.variation
          , min(ld.row_created_at) as first_feature_time
        from launchdarkly.data_export_features ld
        left join web2.users u on ld.user_id = u.okey
        group by 1,2,3)

      --enrich with date time they exited the test (changed variations or never changed)
      --enrich with plan at time of entering test
      select u.effective_user_id
        , u.feature_name
        , u.variation
        , u.first_feature_time
        , pl.id as plan_id_at_time_of_first_feature_time
        , pl.display_name as plan_at_time_of_first_feature_time
        , h.is_sub as is_paid_plan
        , coalesce(dateadd(s,-1,lead(u.first_feature_time,1) over (partition by u.effective_user_id, u.feature_name order by u.first_feature_time)),'2099-01-01 00:00:00') as last_feature_time
      from user_feature_data u
      left join analytics.user_plans_history h on u.effective_user_id = h.user_id
        and u.first_feature_time >= h.starts_at
        and u.first_feature_time < coalesce(h.ends_at_fixed,getdate())
      left join web2.plans pl on h.plan_id = pl.id
      --left join web2.plan_categories pc on pl.category = pc.name
      ;;
    distribution: "effective_user_id"
    sortkeys: ["first_feature_time"]
    sql_trigger_value: SELECT FLOOR(EXTRACT(epoch from GETDATE()) / (1*60*60)) ;; ##trigger for every 1 hour
  }

### DIMENSIONS ###

  dimension: table_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.effective_user_id || ${TABLE}.feature_name || ${TABLE}.variation ;;
  }

  dimension: effective_user_id {
    type: string
    sql: ${TABLE}.effective_user_id ;;
  }

  dimension: is_paid_plan {
    label: "Paid Plan at Time of First Feature Time?"
    type: yesno
    sql: ${TABLE}.is_paid_plan = 1 ;;
  }

  dimension: feature_name {
    type: string
    sql: ${TABLE}.feature_name ;;
  }

  dimension: variation {
    type: string
    sql: ${TABLE}.variation ;;
  }

  dimension_group: first_feature {
    type: time
    sql: ${TABLE}.first_feature_time ;;
  }

  dimension_group: last_feature {
    hidden: yes
    type: time
    sql: ${TABLE}.last_feature_time ;;
  }

  dimension: is_logged_in_user {
    label: "Is Logged In User?"
    hidden: yes
    type: yesno
    sql: len(${effective_user_id}) <= 8 ;;
  }

  dimension: is_new_user {
    hidden: yes
    label: "Is New User? (Free Language Test Only)"
    type: yesno
    sql: ${users_jb.created_date} >= '2021-11-02';;
  }

  dimension: is_new_user2 {
    hidden: yes
    label: "Is New User? (Pricing & Checkout Over Product Test Only)"
    type: yesno
    sql: ${users_jb.created_date} >= '2021-12-15';;
  }

  dimension: is_new_user3 {
    hidden: yes
    label: "Is New User? (Persistent Upsell Test Only)"
    type: yesno
    sql: ${users_jb.created_date} >= '2022-06-07';;
  }

### MEASURES ###

  measure: total_overall_users {
    description: "Includes registered and anonymous users."
    type: count_distinct
    sql: ${effective_user_id} ;;
  }

  measure: total_registered_users {
    type: count_distinct
    sql: ${effective_user_id} ;;
    filters: [is_logged_in_user: "yes"]
  }

  measure: total_anonymous_users {
    description: "Only counts non-registered users."
    type: count_distinct
    sql: ${effective_user_id} ;;
    filters: [is_logged_in_user: "no"]
  }

}
