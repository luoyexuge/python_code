drop  external table if exists staging.bshare_black_ip;
create external table staging.bshare_black_ip (
  antispam_ip varchar(16)
)
location ('gphdfs://genesis4.hadoop.iclick/staging/bshare_blacklist/antispam_ip.txt')
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 100 rows;
begin;
truncate table  xmo_dw.bshare_blacklist_ip;
insert into xmo_dw.bshare_blacklist_ip select  * from staging.bshare_black_ip;
commit;

drop  external table if exists staging.bshare_black_cookie; 
create external table staging.bshare_black_cookie (
  antispam_cookie varchar(50)
)
location ('gphdfs://genesis4.hadoop.iclick/staging/bshare_blacklist/antispam_cookie.txt')
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 100 rows;
begin;
truncate table  xmo_dw.bshare_blacklist_cookie;
insert into  xmo_dw.bshare_blacklist_cookie select * from staging.bshare_black_cookie;
commit;


drop  external  table if exists  staging.bshare_black_tagid;
create external table staging.bshare_black_tagid (
  antispam_tagid varchar(500)
)
location ('gphdfs://genesis4.hadoop.iclick/staging/bshare_blacklist/antispam_tagid.txt')
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 100 rows;
begin;
truncate table xmo_dw.bshare_blacklist_tagid;
insert into xmo_dw.bshare_blacklist_tagid select  * from staging.bshare_black_tagid;
commit;

drop  external  table if exists  staging.bshare_black_domain;
create external table staging.bshare_black_domain (
  antispam_domain varchar(100)
)
location ('gphdfs://genesis4.hadoop.iclick/staging/bshare_blacklist/antispam_domain.txt')
format 'text' (null '' escape 'OFF' fill missing fields)
log errors into staging.hd_ext_load_errors segment reject limit 100 rows;
begin;
truncate table   xmo_dw.bshare_blacklist_domain ;
insert into  xmo_dw.bshare_blacklist_domain select * from  staging.bshare_black_domain;
commit;

