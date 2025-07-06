-- 任务：多个迁移的集合；方便同一时刻按顺序执行多个迁移
create table tasks (
  id integer primary key autoincrement,
  code text not null unique, -- 编码
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null
);
