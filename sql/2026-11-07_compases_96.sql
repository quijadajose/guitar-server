-- =============================================================================
-- Guitar App · 7 de noviembre de 2026
-- Sube el máximo de compases por canción de 32 a 96, para que las canciones
-- largas (p. ej. las transcritas de los videos: Megalovania usa 80) se puedan
-- publicar en la comunidad sin partirlas.
-- Debe coincidir con SONG_LIMITS.measuresMax en guitar-app/src/songSafety.ts.
--
-- ESTADO: BORRADOR. No está ejecutado. Seguro de aplicar: solo amplía el rango,
-- ninguna fila que hoy cumple deja de cumplir. Ejecutar en el SQL editor de Supabase.
-- =============================================================================

alter table public.community_songs drop constraint if exists community_songs_measures_range;
alter table public.community_songs add constraint community_songs_measures_range
  check (measures between 1 and 96) not valid;
