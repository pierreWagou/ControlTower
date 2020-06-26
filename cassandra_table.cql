use romonpie_ks;

drop table if exists time_flight;

create table time_flight (
  year int,
  month int,
  weekday int,
  date timestamp,
  tail_num varchar,
  delay int,
  primary key ((year, month, weekday), date, tail_num)
);
