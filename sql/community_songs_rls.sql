-- Aplicar en el SQL editor de Supabase. La clave publicable del cliente
-- no puede imponer esto sola: la tabla tiene que rechazar filas ajenas.

alter table public.community_songs enable row level security;

drop policy if exists community_songs_public_read on public.community_songs;
create policy community_songs_public_read
  on public.community_songs
  for select
  to anon, authenticated
  using (is_public = true or user_id = auth.uid());

drop policy if exists community_songs_insert_own on public.community_songs;
create policy community_songs_insert_own
  on public.community_songs
  for insert
  to authenticated
  with check (user_id = auth.uid());

drop policy if exists community_songs_update_own on public.community_songs;
create policy community_songs_update_own
  on public.community_songs
  for update
  to authenticated
  using (user_id = auth.uid())
  with check (user_id = auth.uid());

drop policy if exists community_songs_delete_own on public.community_songs;
create policy community_songs_delete_own
  on public.community_songs
  for delete
  to authenticated
  using (user_id = auth.uid());
