create table skipped_messages (
    channel_id text not null,
    message_id bigint not null,
    reason text,
    file_size bigint,
    limit_bytes bigint,
    filename text,
    media_type text,
    skipped_at timestamptz not null default now(),
    primary key (channel_id, message_id)
);
