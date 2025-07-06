-- 数据仓库
create table if not exists repositories (
  id integer primary key autoincrement,
  kind text not null,        -- 仓库类型
  code text not null unique, -- 编码
  properties jsonb not null, -- 连接信息
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  check ( kind in ('postgres', 'odps', 'duckdb', 'sqlite', 'bitable', 'file') )
);
