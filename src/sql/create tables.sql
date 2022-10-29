--Шаг 2
create table DUROVVERYANDEXRU__STAGING.group_log(
    group_id int primary key,
    user_id int ,
    user_id_from int ,
    event varchar(20),
    datetime  datetime
);

--Шаг 4
create table DUROVVERYANDEXRU__DWH.l_user_group_activity(
    hk_l_user_group_activity int primary key,
    hk_user_id int CONSTRAINT fk_hk_user_id REFERENCES DUROVVERYANDEXRU__DWH.h_users (hk_user_id),
    hk_group_id int CONSTRAINT fk_hk_group_id REFERENCES DUROVVERYANDEXRU__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src  varchar(20)
);


--Шаг 6
create table DUROVVERYANDEXRU__DWH.s_auth_history(
    hk_l_user_group_activity int CONSTRAINT fk_hk_user_group REFERENCES DUROVVERYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from int,
    event varchar(20),
    event_dt datetime,
    load_dt datetime,
    load_src  varchar(20)
);