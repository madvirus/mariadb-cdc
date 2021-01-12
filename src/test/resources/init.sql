create table test.member (
  id BIGINT(20) auto_increment,
  name varchar(200),
  email varchar(200),
  use_yn char(1),
  agree boolean,
  description varchar(4000),
  birthday date,
  reg datetime default current_timestamp(),
  primary key(id)
)
COLLATE='utf8_general_ci'
engine = innodb;

create table test.user (
  login_id varchar(200),
  email varchar(200),
  primary key(login_id)
)
COLLATE='utf8_general_ci'
engine = innodb;