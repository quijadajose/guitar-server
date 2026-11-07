-- =============================================================================
-- Guitar App · 7 de noviembre de 2026
--   1. Likes reales (tabla song_likes + contador sincronizado).
--   2. Contador de reproducciones (función register_song_play).
--   3. Respaldo y limpieza de filas inválidas de community_songs.
--   4. Borrado de cuentas programado sin escanear todos los usuarios.
--
-- ESTADO: BORRADOR. No está ejecutado. Aplicar en el SQL editor de Supabase
-- en el orden de las secciones. Las secciones marcadas "NO EJECUTAR" son
-- destructivas o cambian comportamiento: revisar y ejecutar a mano.
-- Supone que community_songs.id es uuid y que likes_count/plays_count existen
-- (el trigger community_songs_rate_limit ya los pone en 0).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Likes
-- -----------------------------------------------------------------------------
create table if not exists public.song_likes (
  user_id    uuid        not null references auth.users(id) on delete cascade,
  song_id    uuid        not null references public.community_songs(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (user_id, song_id)
);

alter table public.song_likes enable row level security;

drop policy if exists song_likes_read on public.song_likes;
create policy song_likes_read
  on public.song_likes for select
  to anon, authenticated
  using (true);

drop policy if exists song_likes_insert_own on public.song_likes;
create policy song_likes_insert_own
  on public.song_likes for insert
  to authenticated
  with check (user_id = (select auth.uid()));

drop policy if exists song_likes_delete_own on public.song_likes;
create policy song_likes_delete_own
  on public.song_likes for delete
  to authenticated
  using (user_id = (select auth.uid()));

-- El contador lo mantiene solo el trigger (security definer). El cliente no
-- puede tocar likes_count: ya está revocado en community_songs.
create or replace function public.sync_song_likes_count()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if tg_op = 'INSERT' then
    update public.community_songs
       set likes_count = likes_count + 1
     where id = new.song_id;
  elsif tg_op = 'DELETE' then
    update public.community_songs
       set likes_count = greatest(likes_count - 1, 0)
     where id = old.song_id;
  end if;
  return null;
end;
$$;

drop trigger if exists song_likes_count on public.song_likes;
create trigger song_likes_count
  after insert or delete on public.song_likes
  for each row execute function public.sync_song_likes_count();

-- Un like por usuario y canción lo garantiza la clave primaria; el doble clic
-- simplemente falla con unique_violation y el cliente lo trata como ya dado.

-- -----------------------------------------------------------------------------
-- 2. Reproducciones
-- Sin dedupe: cualquiera puede inflar plays_count llamando la función. Decisión
-- abierta: limitar por IP (en el servidor) o por usuario y canción por hora.
-- -----------------------------------------------------------------------------
create or replace function public.register_song_play(p_song_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.community_songs
     set plays_count = plays_count + 1
   where id = p_song_id
     and is_public = true;
end;
$$;

revoke all on function public.register_song_play(uuid) from public;
grant execute on function public.register_song_play(uuid) to anon, authenticated;

-- -----------------------------------------------------------------------------
-- 3. Filas inválidas de community_songs
-- Paso A (seguro): respaldo + conteo. No borra nada.
-- Son las filas que incumplen alguna de las restricciones NOT VALID.
-- Ojo: un NULL hace que la condición sea desconocida y la fila no se cuenta;
-- si hay NULLs en title/creator_name/mode, revisarlos aparte.
-- -----------------------------------------------------------------------------
create table if not exists public.community_songs_invalid_backup_20261107 as
select s.*
  from public.community_songs s
 where not (
       char_length(s.title) between 1 and 80
   and (s.section is null or char_length(s.section) <= 80)
   and char_length(s.creator_name) between 1 and 30
   and s.bpm between 50 and 180
   and s.measures between 1 and 32
   and s.mode in ('notes', 'chords')
   and s.difficulty in ('easy', 'medium', 'hard', 'expert')
   and jsonb_typeof(s.notes) = 'array'
   and jsonb_array_length(s.notes) <= 2048
   and pg_column_size(s.notes) <= 262144
   and jsonb_typeof(s.chords) = 'array'
   and jsonb_array_length(s.chords) <= 256
   and pg_column_size(s.chords) <= 65536
   and (s.audio_url is null
        or (s.audio_url ~ '^https://[^\s"<>]+$' and char_length(s.audio_url) <= 500))
 );

-- El respaldo no debe ser legible por la API pública.
alter table public.community_songs_invalid_backup_20261107 enable row level security;
revoke all on public.community_songs_invalid_backup_20261107 from anon, authenticated;

select count(*) as filas_invalidas from public.community_songs_invalid_backup_20261107;

-- Paso B — NO EJECUTAR hasta revisar el conteo y el respaldo:
-- delete from public.community_songs
--  where id in (select id from public.community_songs_invalid_backup_20261107);
--
-- Paso C — NO EJECUTAR hasta el paso B: valida las restricciones NOT VALID.
-- alter table public.community_songs validate constraint community_songs_title_len;
-- alter table public.community_songs validate constraint community_songs_section_len;
-- alter table public.community_songs validate constraint community_songs_creator_len;
-- alter table public.community_songs validate constraint community_songs_bpm_range;
-- alter table public.community_songs validate constraint community_songs_measures_range;
-- alter table public.community_songs validate constraint community_songs_mode_enum;
-- alter table public.community_songs validate constraint community_songs_difficulty_enum;
-- alter table public.community_songs validate constraint community_songs_notes_shape;
-- alter table public.community_songs validate constraint community_songs_chords_shape;
-- alter table public.community_songs validate constraint community_songs_audio_url_https;

-- -----------------------------------------------------------------------------
-- 4. Borrado de cuentas sin escanear usuarios
-- Hoy el servidor guarda deletion_due en app_metadata y purge_scheduled_deletions
-- recorre todos los usuarios. La tabla reemplaza ese recorrido: el cron lee solo
-- las solicitudes vencidas. El servidor debe escribir aquí (service role) en
-- vez de en app_metadata; ese cambio está pendiente en auth.rs.
-- -----------------------------------------------------------------------------
create table if not exists public.account_deletion_requests (
  user_id      uuid        primary key references auth.users(id) on delete cascade,
  requested_at timestamptz not null default now(),
  due_at       timestamptz not null
);

create index if not exists account_deletion_requests_due_idx
  on public.account_deletion_requests (due_at);

-- Solo el servidor (service role) y postgres acceden. Ningún cliente.
alter table public.account_deletion_requests enable row level security;
revoke all on public.account_deletion_requests from anon, authenticated;

-- Borrado efectivo: solo filas vencidas.
-- NO EJECUTAR la función sin revisar: borra usuarios de auth.users.
-- Los audios en Storage no se borran aquí; hay que purgarlos aparte.
create or replace function public.purge_due_account_deletions()
returns integer
language plpgsql
security definer
set search_path = public, auth
as $$
declare
  r record;
  n integer := 0;
begin
  for r in
    select user_id from public.account_deletion_requests where due_at <= now()
  loop
    delete from public.community_songs where user_id = r.user_id;
    delete from auth.users where id = r.user_id;  -- cascada borra la solicitud
    n := n + 1;
  end loop;
  return n;
end;
$$;

revoke all on function public.purge_due_account_deletions() from public, anon, authenticated;

-- pg_cron — NO EJECUTAR hasta revisar la función y probarla con una cuenta de prueba.
-- create extension if not exists pg_cron;
-- select cron.schedule(
--   'purge-account-deletions',
--   '17 3 * * *',
--   $$select public.purge_due_account_deletions();$$
-- );

-- -----------------------------------------------------------------------------
-- 5. Migración de solicitudes existentes (NO EJECUTAR hasta revisar)
-- Antes de este cambio, la fecha vivía en auth.users.raw_app_meta_data.deletion_due.
-- Copia esas solicitudes a la tabla nueva para que el purgado las vea.
-- -----------------------------------------------------------------------------
-- insert into public.account_deletion_requests (user_id, requested_at, due_at)
-- select u.id, now(), (u.raw_app_meta_data->>'deletion_due')::timestamptz
--   from auth.users u
--  where u.raw_app_meta_data ? 'deletion_due'
-- on conflict (user_id) do update set due_at = excluded.due_at;
