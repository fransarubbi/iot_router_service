//! Registro concurrente de conexiones con recolección de basura en segundo plano.
//!
//! Este módulo provee el sistema de estado de memoria principal para el servidor HTTPS/mTLS.
//! Dado que HTTP es un protocolo sin estado (stateless), este registro simula sesiones
//! manteniendo un rastreo de los dispositivos (Hub) que se conectan en modo Bypass.
//!
//! Implementa un mapa concurrente fragmentado (`DashMap`) para garantizar rendimiento
//! de lectura/escritura en tiempo constante (O(1)) incluso bajo alta concurrencia.
//!
//! Además, incluye un patrón "Reaper", una tarea asíncrona que escanea periódicamente
//! y purga automáticamente aquellas conexiones que no han emitido señales de vida
//! dentro del tiempo límite configurado.


use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time;
use tracing::{debug, info, warn};
use uuid::Uuid;


/// Representa el estado actual de un cliente conectado al Router.
///
/// Contiene metadatos de seguridad y estadísticas de red. Es serializable
/// para poder ser expuesto a través de las APIs de administración (ej. `/connections`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// Identificador único (UUID v4) de la conexión en esta sesión.
    pub id: String,
    /// Common Name (CN) extraído del certificado mTLS del cliente.
    pub client_cn: String,
    /// Marca de tiempo exacta del primer contacto (`/connect`).
    pub connected_at: DateTime<Utc>,
    /// Marca de tiempo de la última interacción detectada.
    pub last_seen: DateTime<Utc>,
    /// Dirección IP y puerto del socket remoto del cliente.
    pub remote_addr: String,
    /// Cantidad total de peticiones procesadas durante esta conexión.
    pub requests_count: u64,
}


/// Registro central en memoria para administrar el ciclo de vida de las conexiones.
///
/// Está diseñado para ser clonado libremente entre los distintos hilos y middlewares
/// de Axum, ya que su estado interno está protegido por un `Arc`.
#[derive(Clone)]
pub struct ConnectionRegistry {
    /// Mapa concurrente protegido. La clave es el UUID de la conexión (`String`).
    inner: Arc<DashMap<String, ConnectionInfo>>,
    /// Tiempo máximo que una conexión puede estar inactiva antes de ser considerada "muerta".
    idle_timeout: Duration,
}


impl ConnectionRegistry {

    /// Crea una nueva instancia del registro de conexiones.
    ///
    /// # Argumentos
    ///
    /// * `idle_timeout` - La duración de inactividad tolerada. Superado este tiempo
    /// sin un `touch()`, el Reaper eliminará la conexión en su próximo escaneo.
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            idle_timeout,
        }
    }

    /// Registra un nuevo dispositivo y le asigna una sesión.
    ///
    /// Genera un identificador único (UUID v4) para la sesión, guarda la información inicial,
    /// y emite un log de trazabilidad.
    ///
    /// # Argumentos
    ///
    /// * `client_cn` - Common Name extraído de la verificación mTLS.
    /// * `remote_addr` - IP del socket entrante.
    ///
    /// # Retorna
    ///
    /// El `String` correspondiente al UUID de la conexión registrada.
    pub fn register(&self, client_cn: String, remote_addr: String) -> String {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        self.inner.insert(
            id.clone(),
            ConnectionInfo {
                id: id.clone(),
                client_cn: client_cn.clone(),
                connected_at: now,
                last_seen: now,
                remote_addr: remote_addr.clone(),
                requests_count: 0,
            },
        );
        info!(connection_id = %id, client_cn = %client_cn, remote_addr = %remote_addr, "Info: nueva conexión registrada");
        id
    }

    /// Actualiza la "señal de vida" de una conexión activa.
    ///
    /// Actualiza la estampa `last_seen` a la hora actual (`Utc::now()`) y aumenta
    /// en 1 el contador histórico de peticiones. Esto evita que el Reaper
    /// desconecte prematuramente a un cliente que está emitiendo telemetría.
    ///
    /// # Argumentos
    ///
    /// * `id` - El UUID de la conexión que generó actividad. Si no existe, es ignorado.
    pub fn touch(&self, id: &str) {
        if let Some(mut conn) = self.inner.get_mut(id) {
            conn.last_seen = Utc::now();
            conn.requests_count += 1;
        }
    }

    /// Genera una copia instantánea del estado de todas las conexiones.
    ///
    /// Útil para volcar métricas o exponerlas a través de un endpoint HTTP de administración.
    ///
    /// # Retorna
    ///
    /// Un `Vec` conteniendo clones de las estructuras `ConnectionInfo` activas.
    pub fn snapshot(&self) -> Vec<ConnectionInfo> {
        self.inner.iter().map(|e| e.value().clone()).collect()
    }

    /// Obtiene la cantidad de conexiones mTLS actualmente activas en memoria.
    pub fn count(&self) -> usize {
        self.inner.len()
    }

    /// Inicia el bucle infinito de limpieza de conexiones muertas en un hilo asíncrono.
    ///
    /// Esta función "consume" (toma propiedad de) un clon del registro e inicia una tarea
    /// en Tokio (`tokio::spawn`) que despierta periódicamente según el `interval` indicado.
    ///
    /// Por cada ciclo, escanea todas las conexiones, determina cuáles han excedido el
    /// límite de `idle_timeout` respecto a su `last_seen`, y las expulsa del sistema,
    /// emitiendo logs descriptivos en el proceso.
    ///
    /// # Argumentos
    ///
    /// * `interval` - Cada cuánto tiempo (frecuencia) el Reaper despertará para hacer la limpieza.
    pub fn start_reaper(self, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            loop {
                ticker.tick().await;
                let now = Utc::now();
                let timeout_secs = self.idle_timeout.as_secs() as i64;

                let dead: Vec<String> = self
                    .inner
                    .iter()
                    .filter(|e| {
                        let age = now
                            .signed_duration_since(e.value().last_seen)
                            .num_seconds();
                        age > timeout_secs
                    })
                    .map(|e| e.key().clone())
                    .collect();

                for id in &dead {
                    self.inner.remove(id);
                    warn!(connection_id = %id, "Conexión muerta eliminada por reaper");
                }

                if !dead.is_empty() {
                    info!(
                        reaped = dead.len(),
                        active = self.inner.len(),
                        "Reaper completó limpieza"
                    );
                } else {
                    debug!(active = self.inner.len(), "Reaper: sin conexiones muertas");
                }
            }
        });
    }
}