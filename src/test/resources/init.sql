create table test.member (
  id int auto_increment,
  name varchar(200),
  email varchar(200),
  use_yn char(1),
  agree boolean,
  reg datetime default current_timestamp(),
  primary key(id)
) engine = innodb;

create table test.user (
  login_id varchar(200),
  email varchar(200),
  primary key(login_id)
) engine = innodb;