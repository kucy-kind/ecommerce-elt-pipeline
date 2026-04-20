create table user_monthly_link (
    user_id varchar(50),
    create_date date,
    link TEXT
);
create table user_yearly_link (
    user_id varchar(50),
    create_date date,
    link TEXT
);
create table merchant_daily_link (
    merchant_id varchar(50),
    create_date date,
    link TEXT
);
create table merchant_monthly_link (
    merchant_id varchar(50),
    create_date date,
    link TEXT
);
create table merchant_yearly_link (
    merchant_id varchar(50),
    create_date date,
    link TEXT
);
create table platform_daily_link (
    create_date date,
    link TEXT
);
create table platform_monthly_link (
    create_date date,
    link TEXT
);
create table platform_yearly_link (
    create_date date,
    link TEXT
);