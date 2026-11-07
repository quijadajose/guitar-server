//! Catálogo único de canciones de Versus. Fuente de verdad para el servidor:
//! `events` es el número de notas/acordes que se pueden acertar y `points_per_hit`
//! el puntaje base. El cliente lo consulta en `GET /songs` y se niega a empezar
//! si su partitura local no coincide.

#[derive(Debug, Clone, Copy)]
pub struct SongSpec {
    pub id: &'static str,
    pub events: u32,
    pub points_per_hit: u32,
}

pub const SONGS: &[SongSpec] = &[
    SongSpec { id: "sultans_swing", events: 8, points_per_hit: 150 },
    SongSpec { id: "fur_elise", events: 130, points_per_hit: 150 },
    SongSpec { id: "chords_progression", events: 5, points_per_hit: 300 },
];

pub fn find(id: &str) -> Option<&'static SongSpec> {
    SONGS.iter().find(|song| song.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_and_findable() {
        for song in SONGS {
            assert_eq!(find(song.id).map(|s| s.events), Some(song.events));
        }
        assert!(find("no_existe").is_none());
        let mut ids: Vec<_> = SONGS.iter().map(|s| s.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), SONGS.len());
    }
}
