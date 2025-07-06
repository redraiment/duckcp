-- 开放平台访问凭证：由系统管理
create table if not exists credentials (
  id integer primary key autoincrement,
  platform_code text not null,           -- 开放平台
  app_code text not null,                -- 应用编码
  access_token text default '' not null, -- 访问凭证
  expired_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  unique (platform_code, app_code)
);
