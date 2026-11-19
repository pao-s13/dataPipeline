"""Parámetros de negocio del pipeline. Todos los umbrales son configurables."""
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    # CA 1: clave natural para detectar reintentos de sincronización.
    dedup_key: tuple = ("user_id", "book_id", "event_type", "event_ts", "progress_pct")

    # CA 2: una pausa mayor (estricto) a este valor abre una nueva sesión de lectura.
    session_gap_minutes: int = 30

    # CA 3: rango de progreso (en %) que negocio considera "abandono a la mitad".
    dropoff_min_pct: float = 40.0
    dropoff_max_pct: float = 60.0
    # Días sin actividad (respecto a la fecha de referencia) para considerar abandono.
    dropoff_inactivity_days: int = 30

    # Progreso a partir del cual un libro se considera terminado.
    finished_pct: float = 95.0

    # CA 4: binge = terminado en <= N sesiones o en <= N horas desde la primera
    # apertura (ambos medidos hasta el momento en que se termina).
    binge_max_sessions: int = 1
    binge_max_span_hours: float = 24.0
    # Para contar como binge, la lectura observada debe empezar en este % o antes
    # (evita que un "finish" aislado o una sesión que arranca en 85% cuente).
    binge_max_start_pct: float = 10.0
    # Exigir tiempo de lectura medido > 0 para el binge. En logs de app es la regla;
    # en fuentes dispersas (solo inicio y fin, p. ej. Goodreads) no hay eventos
    # intermedios y el tiempo de lectura no es medible, así que se desactiva.
    binge_require_reading_time: bool = True

    @classmethod
    def sparse_events(cls) -> "PipelineConfig":
        """Preset para fuentes con solo eventos de inicio y fin (sin progreso intermedio).

        * Abandono: como no hay % intermedio, el rango 40-60% no es observable; se
          usa [0, 0] = empezó y nunca registró avance ni fin (proxy de abandono).
        * Binge: se mide por el lapso inicio -> fin (<= 24 h), sin exigir tiempo de
          lectura, que con dos eventos separados por horas siempre es 0.
        """
        return cls(dropoff_min_pct=0.0, dropoff_max_pct=0.0,
                   binge_require_reading_time=False)
