-- 存储单元
create table if not exists storages (
  id integer primary key autoincrement,
  repository_id bigint not null references repositories (id) -- 所属仓库
    on update cascade
    on delete cascade,
  code text not null,                                        -- 编码
  properties jsonb not null,                                 -- 介质信息
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  unique (repository_id, code)
);
