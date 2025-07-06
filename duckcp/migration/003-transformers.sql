-- 数据迁移
create table if not exists transformers (
  id integer primary key autoincrement,
  code text not null unique,                             -- 编码
  source_id bigint not null references repositories (id) -- 数据来源
    on update cascade
    on delete cascade,
  target_id bigint not null references storages (id)     -- 数据去向
    on update cascade
    on delete cascade,
  script_file text not null,                             -- 迁移脚本
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  unique (source_id, target_id)
);
