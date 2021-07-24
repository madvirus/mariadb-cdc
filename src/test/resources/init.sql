create table test.member (
  id BIGINT(20) auto_increment,
  name varchar(200),
  email varchar(200),
  use_yn char(1),
  agree boolean,
  description varchar(4000),
  expdate date,
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

create table test.timedata (
  id BIGINT(20),
  dt datetime,
  da date,
  primary key(id)
)
COLLATE='utf8_general_ci'
engine = innodb;

create table test.item (
  item_id varchar(200),
  item_code varchar(200),
  name varchar(200),
  primary key(item_id),
  unique key(item_code)
)
COLLATE='utf8_general_ci'
engine = innodb;

create table test.item_detail (
  item_id varchar(200),
  item_code varchar(200),
  description varchar(200),
  primary key (item_id),
  constraint fk_item_detail_01 foreign key (item_id) references test.item(item_id) on delete cascade,
  constraint fk_item_detail_02 foreign key (item_code) references test.item(item_code) on update cascade
)
COLLATE='utf8_general_ci'
engine = innodb;
