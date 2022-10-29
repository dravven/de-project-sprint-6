--Шаг 7.1
with user_group_messages as (
    select hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from DUROVVERYANDEXRU__DWH.l_user_message lum
             right join DUROVVERYANDEXRU__DWH.h_users hu on lum.hk_user_id = hu.user_id
             join DUROVVERYANDEXRU__DWH.l_user_group_activity luga on luga.hk_user_id = hu.hk_user_id
    where hk_message_id is not null
    group by hk_group_id
)select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;


--Шаг 7.2
with user_group_log  as(
    select distinct luga.hk_group_id, count(distinct user_id_from) as cnt_added_users from DUROVVERYANDEXRU__DWH.s_auth_history sah
    join DUROVVERYANDEXRU__DWH.l_user_group_activity luga on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    where event = 'add'
    group by luga.hk_group_id
), t2 as(
    select distinct hk_group_id, group_id, registration_dt from DUROVVERYANDEXRU__DWH.h_groups
) select user_group_log .hk_group_id, cnt_added_users from user_group_log
join t2 on user_group_log .hk_group_id = t2.hk_group_id
order by registration_dt
limit 10;


--Шаг 7.3
with user_group_messages as (
    select hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from DUROVVERYANDEXRU__DWH.l_user_message lum
             right join DUROVVERYANDEXRU__DWH.h_users hu on lum.hk_user_id = hu.user_id
             join DUROVVERYANDEXRU__DWH.l_user_group_activity luga on luga.hk_user_id = hu.hk_user_id
    where hk_message_id is not null
    group by hk_group_id
), user_group_log  as(
    select distinct luga.hk_group_id, count(distinct user_id_from) as cnt_added_users from DUROVVERYANDEXRU__DWH.s_auth_history sah
    join DUROVVERYANDEXRU__DWH.l_user_group_activity luga on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    where event = 'add'
    group by luga.hk_group_id
), t2 as(
    select distinct hk_group_id, group_id, registration_dt from DUROVVERYANDEXRU__DWH.h_groups
)
select ugm.hk_group_id, cnt_added_users, cnt_users_in_group_with_messages,  (cnt_users_in_group_with_messages / cnt_added_users)::numeric(14, 2)
from user_group_messages ugm
join user_group_log ugl on ugm.hk_group_id = ugl.hk_group_id
join t2 on t2.hk_group_id = ugm.hk_group_id
order by registration_dt, cnt_users_in_group_with_messages
limit 10;
