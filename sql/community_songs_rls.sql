-- Aplicar en el SQL editor de Supabase. La clave publicable del cliente
-- no puede imponer esto sola: la tabla tiene que rechazar filas ajenas.

alter table public.community_songs enable row level security;
alter table public.community_songs force row level security;

drop policy if exists community_songs_public_read on public.community_songs;
create policy community_songs_public_read
  on public.community_songs
  for select
  to anon, authenticated
  using (is_public = true or user_id = (select auth.uid()));

drop policy if exists community_songs_insert_own on public.community_songs;
create policy community_songs_insert_own
  on public.community_songs
  for insert
  to authenticated
  with check (user_id = (select auth.uid()));

drop policy if exists community_songs_update_own on public.community_songs;
create policy community_songs_update_own
  on public.community_songs
  for update
  to authenticated
  using (user_id = (select auth.uid()))
  with check (user_id = (select auth.uid()));

drop policy if exists community_songs_delete_own on public.community_songs;
create policy community_songs_delete_own
  on public.community_songs
  for delete
  to authenticated
  using (user_id = (select auth.uid()));

-- ---------------------------------------------------------------------------
-- Columnas que el cliente NO debe poder escribir.
-- Con RLS sola, un usuario podía insertar likes_count = 999999 o cambiar
-- created_at para quedar primero en la lista.
-- ---------------------------------------------------------------------------
revoke insert, update on public.community_songs from anon;
revoke insert, update on public.community_songs from authenticated;
grant insert (user_id, creator_name, title, section, bpm, mode, difficulty,
              measures, notes, chords, audio_url, is_public)
  on public.community_songs to authenticated;
grant update (creator_name, title, section, bpm, mode, difficulty,
              measures, notes, chords, audio_url, is_public)
  on public.community_songs to authenticated;

-- ---------------------------------------------------------------------------
-- Límites de contenido. El cliente ya los aplica (songSafety.ts), pero
-- cualquiera puede llamar a la API REST directamente con la clave pública.
-- NOT VALID: no falla por filas viejas; se validan solo las nuevas.
-- Cuando limpies los datos viejos: alter table ... validate constraint ...;
-- ---------------------------------------------------------------------------
alter table public.community_songs drop constraint if exists community_songs_title_len;
alter table public.community_songs add constraint community_songs_title_len
  check (char_length(title) between 1 and 80) not valid;

alter table public.community_songs drop constraint if exists community_songs_section_len;
alter table public.community_songs add constraint community_songs_section_len
  check (section is null or char_length(section) <= 80) not valid;

alter table public.community_songs drop constraint if exists community_songs_creator_len;
alter table public.community_songs add constraint community_songs_creator_len
  check (char_length(creator_name) between 1 and 30) not valid;

alter table public.community_songs drop constraint if exists community_songs_bpm_range;
alter table public.community_songs add constraint community_songs_bpm_range
  check (bpm between 50 and 180) not valid;

alter table public.community_songs drop constraint if exists community_songs_measures_range;
alter table public.community_songs add constraint community_songs_measures_range
  check (measures between 1 and 32) not valid;

alter table public.community_songs drop constraint if exists community_songs_mode_enum;
alter table public.community_songs add constraint community_songs_mode_enum
  check (mode in ('notes', 'chords')) not valid;

alter table public.community_songs drop constraint if exists community_songs_difficulty_enum;
alter table public.community_songs add constraint community_songs_difficulty_enum
  check (difficulty in ('easy', 'medium', 'hard', 'expert')) not valid;

alter table public.community_songs drop constraint if exists community_songs_notes_shape;
alter table public.community_songs add constraint community_songs_notes_shape
  check (jsonb_typeof(notes) = 'array'
         and jsonb_array_length(notes) <= 2048
         and pg_column_size(notes) <= 262144) not valid;

alter table public.community_songs drop constraint if exists community_songs_chords_shape;
alter table public.community_songs add constraint community_songs_chords_shape
  check (jsonb_typeof(chords) = 'array'
         and jsonb_array_length(chords) <= 256
         and pg_column_size(chords) <= 65536) not valid;

-- audio_url solo https (nada de javascript:, data:, http: plano).
alter table public.community_songs drop constraint if exists community_songs_audio_url_https;
alter table public.community_songs add constraint community_songs_audio_url_https
  check (audio_url is null
         or (audio_url ~ '^https://[^\s"<>]+$' and char_length(audio_url) <= 500)) not valid;

-- ---------------------------------------------------------------------------
-- Tope de publicaciones por usuario (anti-spam): 20 por día.
-- ---------------------------------------------------------------------------
create or replace function public.community_songs_rate_limit()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if (select count(*) from public.community_songs
       where user_id = new.user_id
         and created_at > now() - interval '1 day') >= 20 then
    raise exception 'Límite de publicaciones diario alcanzado'
      using errcode = 'P0001';
  end if;
  new.created_at := now();
  new.likes_count := 0;
  new.plays_count := 0;
  return new;
end;
$$;

drop trigger if exists community_songs_rate_limit on public.community_songs;
create trigger community_songs_rate_limit
  before insert on public.community_songs
  for each row execute function public.community_songs_rate_limit();

create index if not exists community_songs_user_created_idx
  on public.community_songs (user_id, created_at desc);
