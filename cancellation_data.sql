
-- Seller level base table to segment the sellers based on the no of cancellation, cancellation rate 

drop table if exists analytics_scratch.ashika_cancellation_behaviour_data_updated;
create table analytics_scratch.ashika_cancellation_behaviour_data_updated as
 Select
       seller_id,
       username,
       platform_user_id,
       total_seller_reason_cancellation_rate_Bucket,
       max_seller_Reason_cancellation_rate_Bucket,
       max_seller_reason_Cancelled_orders,
       total_seller_reason_cancelled_orders_yearly,
       sum(orders)                            as booked_orders,
       sum(cancelled_orders)                  as total_cancelled_orders,
       sum(seller_reason_cancelled_orders)    as seller_reason_Cancelled_orders

    from ( Select *,
               case when seller_reason_cancellation_rate::float = 0     then 'a. 0'
                    when seller_reason_cancellation_rate::float < 0.05  then 'b. 1-5%'
                    when seller_reason_cancellation_rate::float < 0.1   then 'c. 5-10%'
                    when seller_reason_cancellation_rate::float < 0.2   then 'd. 11-20%'
                    when seller_reason_cancellation_rate::float < 0.4   then 'e. 21-40%'
                    when seller_reason_cancellation_rate::float < 0.6   then 'f. 41-60%'
                    when seller_reason_cancellation_rate::float < 0.8   then 'g. 61-80%'
               else 'h. >80%' end                                                                                                           as seller_reason_cancellation_rate_bucket,

               sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)                           as total_seller_orders_yearly,
               sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)   as total_seller_reason_cancelled_orders_yearly,

               sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
               sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float                    as total_seller_reason_cancellation_rate_yearly,

               case when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float = 0     then 'a. 0'
                    when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float < 0.05  then 'b. 1-5%'
                    when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float < 0.1   then 'c. 5-10%'
                    when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float < 0.2   then 'd. 11-20%'
                    when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float < 0.4   then 'e. 21-40%'
                    when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float < 0.6   then 'f. 41-60%'
                    when sum(seller_reason_cancelled_orders) over (partition by seller_id rows between unbounded preceding and unbounded following)/
                         sum(orders) over (partition by seller_id rows between unbounded preceding and unbounded following)::float < 0.8   then 'g. 61-80%'
               else 'h. >80%' end                                                             as total_seller_reason_cancellation_rate_bucket,


               max(seller_reason_cancelled_orders) over (partition by seller_id)              as max_seller_reason_Cancelled_orders,
               max(seller_reason_cancellation_rate) over (partition by seller_id)             as max_seller_reason_Cancellation_rate,

               case when max(seller_reason_cancellation_rate) over (partition by seller_id)::float = 0     then 'a. 0'
                    when max(seller_reason_cancellation_rate) over (partition by seller_id)::float < 0.05  then 'b. 1-5%'
                    when max(seller_reason_cancellation_rate) over (partition by seller_id)::float < 0.1   then 'c. 5-10%'
                    when max(seller_reason_cancellation_rate) over (partition by seller_id)::float < 0.2   then 'd. 11-20%'
                    when max(seller_reason_cancellation_rate) over (partition by seller_id)::float < 0.4   then 'e. 21-40%'
                    when max(seller_reason_cancellation_rate) over (partition by seller_id)::float < 0.6   then 'f. 41-60%'
                    when max(seller_reason_cancellation_rate) over (partition by seller_id)::float < 0.8   then 'g. 61-80%'
               else 'h. >80%' end                                                             as max_seller_reason_cancellation_rate_bucket


        From(
              Select *,
                     (cancelled_orders - non_seller_reason_cancelled_order)                   as seller_reason_cancelled_orders,
                     (cancelled_orders - non_seller_reason_cancelled_order)/orders::float     as seller_reason_cancellation_rate
              From(

                  SELECT
                      seller.user_id                                                                                            AS seller_id,
                      seller_info.username                                                                                      AS username,
                      seller_info.platform_user_id                                                                              AS platform_user_id,
                      (TO_CHAR(DATE_TRUNC('month', dw_orders.booked_at ), 'YYYY-MM'))                                           AS booked_month,
                      COUNT(DISTINCT dw_orders.order_id )                                                                       AS orders,
                      COUNT(DISTINCT CASE WHEN ( dw_orders.cancelled_on   IS NOT NULL) THEN dw_orders.order_id  ELSE NULL END)  AS cancelled_orders,

                      COUNT(DISTINCT CASE WHEN ((( dw_orders.cancelled_reason  ) ILIKE 'accidental_purchase_by_buyer' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'bundle_with_other_order' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'bundle_with_other_order?cancel_r' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'fraud_ato' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'frd' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'frd_ccf' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'frd_sls' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'rcvd_nt_hpy' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'snt' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'dcl' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'psi' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'rts' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'item_is_lost' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'pmhq_rnh' OR
                                                 ( dw_orders.cancelled_reason  ) ILIKE 'buyer_request')) AND
                                                ( dw_orders.cancelled_on   IS NOT NULL)
                                          THEN dw_orders.order_id  ELSE NULL END)                                               AS non_seller_reason_cancelled_order

                  FROM analytics.dw_orders  AS dw_orders
                  LEFT JOIN analytics.dw_users  AS seller ON seller.user_id = dw_orders.seller_id
                  LEFT JOIN analytics.dw_users_info  AS seller_info ON seller_info.user_id = seller.user_id
                  WHERE ((( dw_orders.booked_at  ) >= (TIMESTAMP '2024-07-01') AND
                          ( dw_orders.booked_at  ) < (TIMESTAMP '2025-07-01'))) AND
                         (( seller.home_domain  ) ILIKE  'us') AND
                          (NOT (coalesce(seller.user_status = 'restricted', FALSE) ) OR
                               (coalesce(seller.user_status = 'restricted', FALSE) ) IS NULL) AND
                        (dw_orders.is_valid_order = TRUE )
                  GROUP BY 1,2,3,4
                )
            )
          order by seller_id, booked_month
  )
group by 1,2,3,4,5,6,7
         order by total_seller_reason_cancelled_orders_yearly;



----  Joining the seller to the order table calculate the total orders, cancelled orders, seller cancelled reason



drop table if exists analytics_scratch.ashika_cancellation_behaviour_data_orders;
create table analytics_scratch.ashika_cancellation_behaviour_data_orders as
select a.seller_id,
       booked_orders,
       CASE WHEN booked_orders =1  THEN 'a. 1 order'
                                     WHEN booked_orders = 2 THEN 'b. 2 orders'
                                    WHEN booked_orders >2 and booked_orders <= 10 THEN 'c. 3 to 10 orders'
                                    WHEN booked_orders > 10 and booked_orders <= 50 THEN 'd. 10 to 50 orders'
                                    WHEN booked_orders > 50 and booked_orders <= 100 THEN 'e. 50 to 100 orders'
                                    WHEN booked_orders >100 and booked_orders <= 500 THEN 'f. 100 to 500 orders'
                                     WHEN booked_orders >500 and booked_orders <= 1000 THEN 'g. 500 to 1000 orders'
                                     WHEN booked_orders >1000 and booked_orders <= 5000 THEN 'h. 1000 to 5000 orders'
                                    WHEN booked_orders >5000 and booked_orders <= 10000 THEN 'i. 5000 to 10000 orders'
                                    WHEN booked_orders >10000  THEN 'j. above 10000 orders'
                                END AS total_orders_segment,
       total_seller_reason_cancellation_rate_bucket,
       total_cancelled_orders,
       total_seller_reason_cancelled_orders_yearly,
       CASE WHEN total_seller_reason_cancelled_orders_yearly = 0 THEN 'a. 0'
            WHEN total_seller_reason_cancelled_orders_yearly = 1 THEN 'b. 1'
           WHEN total_seller_reason_cancelled_orders_yearly = 2 THEN 'c. 2'
           WHEN total_seller_reason_cancelled_orders_yearly = 3 THEN 'd. 3'
           WHEN total_seller_reason_cancelled_orders_yearly = 4 THEN 'e. 4'
           WHEN total_seller_reason_cancelled_orders_yearly = 5 THEN 'f. 5'
           WHEN total_seller_reason_cancelled_orders_yearly > 5 THEN 'g. >5'
        END AS total_seller_reason_cancelled_orders_yearly_bucket,
--       b.listing_id,
--       CASE WHEN parent_listing_id is not null THEN 'Yes' ELSE 'No' END AS is_child_item,
--       coalesce(b.parent_listing_id,b.listing_id) as parent_listing_id,
--       first_published_at,
--       CASE WHEN parent_listing_id is not null THEN parent_first_published_at ELSE  first_published_at
--           END AS parent_first_published_at,
      c.order_id,
      c.listing_id,
      c.booked_at_time,
      e.cancelled_on,
      e.cancelled_reason,
      e.is_valid_order,
      e.shipped_at,
--              case when  datediff('day', first_published_at::date,booked_at_time:: DATE )=0 then '1. D1'
--           when  datediff('day', first_published_at::date,booked_at_time:: DATE ) between 1 and 6 then '2. D2-D7'
--           when  datediff('day', first_published_at::date,booked_at_time:: DATE ) between 7 and 29 then '3. D8-D30'
--           when  datediff('day', first_published_at::date,booked_at_time:: DATE ) between 30 and 89 then '4. D30-D90'
--           when  datediff('day', first_published_at::date,booked_at_time:: DATE ) between 90 and 179  then '5. D90-D180'
--           when  datediff('day', first_published_at::date,booked_at_time:: DATE ) >= 180 then '6. D180+'
--         end as LISTING_AGE_TIER,
    CASE WHEN ((( e.cancelled_reason  ) ILIKE 'accidental_purchase_by_buyer' OR
                                                 ( e.cancelled_reason  ) ILIKE 'bundle_with_other_order' OR
                                                 ( e.cancelled_reason  ) ILIKE 'bundle_with_other_order?cancel_r' OR
                                                 ( e.cancelled_reason  ) ILIKE 'fraud_ato' OR
                                                 ( e.cancelled_reason  ) ILIKE 'frd' OR
                                                 ( e.cancelled_reason  ) ILIKE 'frd_ccf' OR
                                                 ( e.cancelled_reason  ) ILIKE 'frd_sls' OR
                                                 ( e.cancelled_reason  ) ILIKE 'rcvd_nt_hpy' OR
                                                 ( e.cancelled_reason  ) ILIKE 'snt' OR
                                                 ( e.cancelled_reason  ) ILIKE 'dcl' OR
                                                 ( e.cancelled_reason  ) ILIKE 'psi' OR
                                                 ( e.cancelled_reason  ) ILIKE 'rts' OR
                                                 ( e.cancelled_reason  ) ILIKE 'item_is_lost' OR
                                                 ( e.cancelled_reason  ) ILIKE 'pmhq_rnh' OR
                                                 ( e.cancelled_reason  ) ILIKE 'buyer_request') AND
                                                ( e.cancelled_on   IS NOT NULL))
                                          THEN 'No' ELSE 'Yes' END                                               AS is_seller_fault
--       count(distinct date(event_date) ) as no_of_days_active,
--       min(event_date)

       from analytics_scratch.ashika_cancellation_behaviour_data_updated as a
        left join analytics.dw_order_items as c on a.seller_id = c.seller_id
           left join analytics.dw_orders as e on e.order_id = c.order_id
        where  c.order_id is not null and (date(booked_at_time) >= '2024-07-01' and date(booked_at_time) < '2025-07-01')
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;



-- Joining the table to listing table to add the listings age dimansion

drop table if exists analytics_scratch.ashika_cancellation_behaviour_data_listing;
create table analytics_scratch.ashika_cancellation_behaviour_data_listing as
select a.*,
      CASE WHEN parent_listing_id is not null THEN 'Yes' ELSE 'No' END AS is_child_item,
      coalesce(b.parent_listing_id,b.listing_id) as platform_listing_id,
      parent_listing_id,
      first_published_at,
      CASE WHEN parent_listing_id is not null THEN parent_first_published_at ELSE  first_published_at
          END AS platform_first_published_at,
    parent_first_published_at

--       count(distinct date(event_date) ) as no_of_days_active,
--       min(event_date)

       from analytics_scratch.ashika_cancellation_behaviour_data_orders as a
              left join analytics.dw_listings as b on a.listing_id = b.listing_id
--         left join (select * from analytics.dw_user_events_daily where (date(event_date) >= '2024-07-01' and date(event_date) < '2025-07-01')
--                                 and is_active is true )as d on a.seller_id = d.user_id and date(booked_at_time) <= date(event_date)
order by seller_id, booked_at_time asc;




--- Added the Seller activation segment as the above table ( Done all the tables separately because of the WLM error)

drop table if exists analytics_scratch.ashika_cancellation_behaviour_data_final;
create table analytics_scratch.ashika_cancellation_behaviour_data_final as
SELECT a.*,
       case when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE )=0 then '1. D1'
          when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE ) between 1 and 6 then '2. D2-D7'
          when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE ) between 7 and 29 then '3. D8-D30'
          when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE ) between 30 and 59 then '4. D30-D60'
           when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE ) between 60 and 89 then '4. D60-D90'
          when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE ) between 90 and 179  then '5. D90-D180'
          when  datediff('day', parent_first_published_at::date,booked_at_time:: DATE ) >= 180 then '6. D180+'
        end as LISTING_AGE_TIER,
--         case when datediff('day', booked_at_time::date,min:: DATE )=0 then '1. D1'
--           when  datediff('day', booked_at_time::date,min:: DATE ) between 1 and 6 then '2. D2-D7'
--           when  datediff('day', booked_at_time::date,min:: DATE ) between 7 and 29 then '3. D8-D30'
--           when  datediff('day', booked_at_time::date,min:: DATE ) between 30 and 89 then '4. D30-D90'
--           when  datediff('day', booked_at_time::date,min:: DATE ) between 90 and 179  then '5. D90-D180'
--           when  datediff('day', booked_at_time::date,min:: DATE ) >= 180 then '6. D180+'
--          ELSE '7. Not active till August 2025'
-- END AS seller_active_tier_after_order_placed,
        case when date(seller_activated_at) between '2024-07-01' AND '2025-06-30' THEN 'new_seller'
        else 'repeat_seller'
            end as seller_type,
        min(booked_at_time) over (partition by seller_id rows between unbounded preceding and unbounded following ) as first_order_booked_at

    from analytics_scratch.ashika_cancellation_behaviour_data_listing as a
      left join analytics.dw_users on seller_id = user_id
order by seller_id, booked_at_time asc;





------ To identify  the behaviour of the sellers with 1 or 2 cancellation based on the total_seller_reason_cancellation_rate_bucket



select total_seller_reason_cancellation_rate_bucket,
       total_seller_reason_cancelled_orders_yearly,
      LISTING_AGE_TIER,
      is_seller_fault,
       count(distinct seller_id) as sellers,
       count(distinct order_id)  as seller_reason_cancelled_orders
  from analytics_scratch.ashika_cancellation_behaviour_data_final as dw_orders
where dw_orders.cancelled_on is not null and is_seller_fault = 'Yes'
and total_seller_reason_cancelled_orders_yearly > 0 and total_seller_reason_cancelled_orders_yearly <= 2
group by 1,2,3,4
UNION ALL

select total_seller_reason_cancellation_rate_bucket,
       total_seller_reason_cancelled_orders_yearly,
        'All' AS LISTING_AGE_TIER,
        is_seller_fault,
       count(distinct seller_id) as sellers,
       count(distinct order_id)  as seller_reason_cancelled_orders
  from analytics_scratch.ashika_cancellation_behaviour_data_final as dw_orders
where dw_orders.cancelled_on is not null and is_seller_fault = 'Yes'
and total_seller_reason_cancelled_orders_yearly > 0 and total_seller_reason_cancelled_orders_yearly <= 2
group by 1,2,3,4


----- with respect to the total order segemnt



SELECT total_orders_segment,
       seller_type,
       LISTING_AGE_TIER,
       total_seller_reason_cancelled_orders_yearly_bucket,
       CASE WHEN first_order_booked_at = booked_at_time THEN 'Yes'
           ELSE 'No' END AS first_order_of_given_time_window,

       count( distinct seller_id) as seller,
       count(distinct order_id) as orders,
       count(distinct case when cancelled_on is not null then order_id end) as cancelled_orders,
       count(distinct case when cancelled_on is not null and is_seller_fault = 'Yes' then order_id end) as seller_reason_cancelled_orders

        from analytics_scratch.ashika_cancellation_behaviour_data_final

    group by 1,2,3,4,5
order by 1,2,3,4,5


  --- With cancel reason 

  
SELECT total_orders_segment,
       seller_type,
       LISTING_AGE_TIER,
       total_seller_reason_cancelled_orders_yearly_bucket,
       total_seller_reason_cancellation_rate_bucket,
       cancelled_reason,

       CASE WHEN first_order_booked_at = booked_at_time THEN 'Yes'
           ELSE 'No' END AS first_order_of_given_time_window,

       count( distinct seller_id) as seller,
       count(distinct order_id) as orders,
       count(distinct case when cancelled_on is not null then order_id end) as cancelled_orders,
       count(distinct case when cancelled_on is not null and is_seller_fault = 'Yes' then order_id end) as seller_reason_cancelled_orders

        from analytics_scratch.ashika_cancellation_behaviour_data_final

    group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7;

