-- 数据快照：由系统管理
create table if not exists snapshots (
  id integer primary key autoincrement,
  storage_id bigint not null unique references storages (id) -- 所属存储单元
    on update cascade
    on delete cascade,
  checksum text default '' not null,                         -- 摘要
  records jsonb default '[]' not null,                       -- 记录
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null
);
