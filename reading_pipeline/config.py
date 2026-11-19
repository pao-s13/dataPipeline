"""Parámetros de negocio del pipeline. Todos los umbrales son configurables."""
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    # CA 1: clave natural para detectar reintentos de sincronización.
    dedup_key: tuple = ("user_id", "book_id", "event_type", "event_ts", "progress_pct")

    # CA 2: una pausa mayor a este valor abre una nueva sesión de lectura.
    session_gap_minutes: int = 30

    # CA 3: rango de progreso (en %) que negocio considera "abandono a la mitad".
    dropoff_min_pct: float = 40.0
    dropoff_max_pct: float = 60.0
    # Días sin actividad (respecto a la fecha de referencia) para considerar abandono.
    dropoff_inactivity_days: int = 30

    # Progreso a partir del cual un libro se considera terminado.
    finished_pct: float = 95.0

    # CA 4: binge = terminado en <= N sesiones o en un lapso <= N horas.
    binge_max_sessions: int = 1
    binge_max_span_hours: float = 24.0
