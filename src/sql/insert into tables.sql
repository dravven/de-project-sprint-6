--Шаг 5
INSERT INTO DUROVVERYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct hash(hk_group_id, hk_user_id), hk_user_id, hk_group_id, now(), hg.load_src
from DUROVVERYANDEXRU__STAGING.group_log gl
left join DUROVVERYANDEXRU__DWH.h_users hu on gl.user_id = hu.user_id
left join DUROVVERYANDEXRU__DWH.h_groups hg on gl.group_id = hg.group_id;
--Шаг 6
insert into DUROVVERYANDEXRU__DWH.s_auth_history
select hk_l_user_group_activity, user_id_from, event, gl.datetime, now(), hg.load_src
from DUROVVERYANDEXRU__STAGING.group_log as gl
left join DUROVVERYANDEXRU__DWH.h_groups as hg on gl.group_id = hg.group_id
left join DUROVVERYANDEXRU__DWH.h_users as hu on gl.user_id = hu.user_id
left join DUROVVERYANDEXRU__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id

