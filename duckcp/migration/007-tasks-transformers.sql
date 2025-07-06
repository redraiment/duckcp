-- 定时任务关联的迁移
create table if not exists tasks_transformers (
  task_id bigint not null references tasks (id)               -- 所属任务
    on update cascade
    on delete cascade,
  transformer_id bigint not null references transformers (id) -- 所属迁移
    on update cascade
    on delete cascade,
  sort int not null,                                          -- 执行顺序
  created_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  updated_at timestamp default (datetime(current_timestamp, 'localtime')) not null,
  primary key (task_id, transformer_id),
  unique (transformer_id, task_id)
);
