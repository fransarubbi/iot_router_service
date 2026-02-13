//! # Módulo de Enrutamiento (Router)
//!
//! Este módulo constituye el **cerebro y núcleo central** de la aplicación.
//! Su responsabilidad principal es orquestar la comunicación entre los diferentes actores del sistema
//! (Dispositivos Edge, Manager y Data Saver) sin que ellos se conozcan entre sí.
//!
//! ## Arquitectura: Modelo de Actores
//! Este módulo implementa un patrón similar al modelo de actores, donde:
//! 1. **Estado Compartido ([`RoutingTable`]):** Mantiene un registro en tiempo real de quién está conectado.
//! 2. **Bus de Mensajes ([`RouterMessage`]):** Un canal unificado por donde viajan todas las solicitudes internas.
//! 3. **Actor Despachador ([`dispatcher_task`]):** Una tarea asíncrona que procesa los mensajes uno a uno y decide su destino.
//!
//! ## Características Clave
//! * **Thread-Safety:** Utiliza `Arc<RwLock<...>>` para permitir el acceso concurrente seguro desde múltiples hilos de gRPC.
//! * **Packet Inspection:** El despachador inspecciona el contenido de los mensajes (ej. Heartbeats) para cambiar dinámicamente entre envío Unicast y Broadcast.
//! * **Desacoplamiento:** Los servicios gRPC no necesitan saber cómo entregar un mensaje, solo lo depositan en el bus central.


use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tonic::{Status};
use tracing::{debug, error, info, instrument, warn};
use crate::grpc::{ToEdge, ToManager, ToDataSaver, to_edge, Heartbeat};


/// **Mensaje Interno del Router**
///
/// Este Enum actúa como un "sobre" unificado que permite pasar cualquier tipo de mensaje
/// a través del canal central (`central_tx`) hacia el despachador.
/// Ayuda a desacoplar la recepción gRPC de la lógica de enrutamiento.
#[derive(Debug, Clone)]
pub enum RouterMessage {
    /// Mensaje destinado a una Raspberry Pi específica.
    /// Nota: Si el payload contiene un Heartbeat, el Dispatcher lo convertirá en Broadcast.
    ToEdge {
        destination_edge_id: String,
        message: ToEdge,
    },
    /// Mensaje destinado al servicio Manager (Java).
    ToManager {
        message: ToManager,
    },
    /// Mensaje destinado al servicio Data Science (Python).
    ToData {
        message: ToDataSaver,
    },
}


/// **Tabla de Enrutamiento (Thread-Safe)**
///
/// Almacena las conexiones activas (canales mpsc) hacia los diferentes actores del sistema.
/// Utiliza `RwLock` para permitir múltiples lecturas simultáneas (alto rendimiento)
/// y escrituras exclusivas solo cuando se conecta/desconecta alguien.
pub struct RoutingTable {
    /// Mapa de conexiones Edge. Clave: ID del dispositivo, Valor: Canal de envío.
    pub edges: RwLock<HashMap<String, mpsc::Sender<Result<ToEdge, Status>>>>,

    /// Canal único hacia el Manager. Es Option porque puede no estar conectado.
    pub manager: RwLock<Option<mpsc::Sender<Result<ToManager, Status>>>>,

    /// Canal único hacia Data Saver. Es Option porque puede no estar conectado.
    pub data: RwLock<Option<mpsc::Sender<Result<ToDataSaver, Status>>>>,
}


impl RoutingTable {

    /// Crea una nueva instancia envuelta en Arc para ser compartida entre hilos.
    pub fn new() -> Arc<Self> {
        Arc::new(
            Self {
                edges: RwLock::new(HashMap::new()),
                manager: RwLock::new(None),
                data: RwLock::new(None),
            }
        )
    }

    /// **Registrar Edge**
    ///
    /// Verifica si un Edge ya existe. Si no, guarda su canal de comunicación.
    /// Retorna `true` si es una nueva conexión, `false` si ya existía (re-conexión).
    pub async fn ensure_edge_registered(&self,
                                        edge_id: &str,
                                        tx: mpsc::Sender<Result<ToEdge, Status>>,
    ) -> bool {

        let mut edges = self.edges.write().await;
        if edges.contains_key(edge_id) {
            false
        } else {
            edges.insert(edge_id.to_string(), tx);
            info!("Info: edge con id: {} registrado", edge_id);
            true
        }
    }

    /// Elimina la conexión del Edge (ej: al desconectarse).
    pub async fn unregister_edge(&self, edge_id: &str) {
        self.edges.write().await.remove(edge_id);
        info!("Info: edge con id: {} eliminado", edge_id);
    }

    /// Registra la conexión del Manager.
    pub async fn register_manager(&self, tx: mpsc::Sender<Result<ToManager, Status>>) {
        *self.manager.write().await = Some(tx);
        info!("Info: manager registrado");
    }

    /// Elimina la conexión del Manager (ej: al desconectarse).
    pub async fn unregister_manager(&self) {
        *self.manager.write().await = None;
        info!("Info: manager eliminado");
    }

    /// Registra la conexión del Data Saver.
    pub async fn register_data(&self, tx: mpsc::Sender<Result<ToDataSaver, Status>>) {
        *self.data.write().await = Some(tx);
        info!("Info: data registrado");
    }

    /// Elimina la conexión del Data Saver.
    pub async fn unregister_data(&self) {
        *self.data.write().await = None;
        warn!("Info: data eliminado");
    }

    /// **Enviar Unicast a Edge**
    ///
    /// Busca el canal de un Edge específico y le envía el mensaje.
    /// Retorna error si el Edge no está conectado o el canal se cerró.
    pub async fn send_to_edge(&self, edge_id: &str, msg: ToEdge) -> Result<(), String> {
        let edges = self.edges.read().await;

        if let Some(tx) = edges.get(edge_id) {
            tx.send(Ok(msg)).await
                .map_err(|_| format!("Error: canal con edge id: {} cerrado", edge_id))?;
            Ok(())
        } else {
            Err(format!("Error: edge con id: {} no encontrado", edge_id))
        }
    }

    pub async fn send_to_manager(&self, msg: ToManager) -> Result<(), String> {
        let manager = self.manager.read().await;

        if let Some(tx) = manager.as_ref() {
            tx.send(Ok(msg)).await
                .map_err(|_| "Error: canal con manager cerrado".to_string())?;
            Ok(())
        } else {
            Err("Error: manager no conectado".to_string())
        }
    }

    pub async fn send_to_data(&self, msg: ToDataSaver) -> Result<(), String> {
        let data = self.data.read().await;

        if let Some(tx) = data.as_ref() {
            tx.send(Ok(msg)).await
                .map_err(|_| "Error: canal con data cerrado".to_string())?;
            Ok(())
        } else {
            Err("Error: data no conectado".to_string())
        }
    }

    /// **Broadcast de Heartbeat**
    ///
    /// Itera sobre TODOS los edges conectados y les envía un mensaje Heartbeat.
    /// IMPORTANTE: Itera directamente sobre el mapa para evitar Deadlocks (no llama a send_to_edge).
    pub async fn broadcast_heartbeat_to_edges(&self, heartbeat: Heartbeat) -> usize {
        let edges = self.edges.read().await;
        let mut sent_count = 0;

        for (edge_id, tx) in edges.iter() {
            let to_edge = ToEdge {
                edge_id: edge_id.clone(),
                payload: Some(to_edge::Payload::Heartbeat(heartbeat.clone())),
            };

            if tx.send(Ok(to_edge)).await.is_ok() {
                sent_count += 1;
            }
        }
        sent_count
    }
}


/// **Tarea del Despachador (Dispatcher)**
///
/// Es el corazón lógico del router. Recibe mensajes del canal central y decide su destino.
/// Implementa la lógica de "Packet Inspection" para convertir mensajes unicast en broadcast
/// si detecta que son Heartbeats.
#[instrument(
    name = "dispatcher_task",
    skip(rx, routing_table)
)]
pub async fn dispatcher_task(mut rx: mpsc::Receiver<RouterMessage>,
                             routing_table: Arc<RoutingTable>) {

    info!("Info: dispatcher iniciado");

    while let Some(msg) = rx.recv().await {
        match msg {
            RouterMessage::ToEdge { destination_edge_id, message } => {
                if let Some(to_edge::Payload::Heartbeat(hb)) = &message.payload {
                    let count = routing_table.broadcast_heartbeat_to_edges(hb.clone()).await;
                    debug!("Debug: Broadcast enviado a {} dispositivos", count);
                }
                else {
                    debug!("Debug: Enrutando unicast a edge {}", destination_edge_id);
                    if let Err(e) = routing_table.send_to_edge(&destination_edge_id, message).await {
                        error!("Error: no se pudo enviar mensaje a edge {}. {}", destination_edge_id, e);
                    }
                }
            }

            RouterMessage::ToManager { message } => {
                debug!("Debug: enrutando a manager");

                if let Err(e) = routing_table.send_to_manager(message).await {
                    error!("Error: no se pudo enviar mensaje a Manager. {}", e);
                }
            }

            RouterMessage::ToData { message } => {
                debug!("Debug: enrutando a data");

                if let Err(e) = routing_table.send_to_data(message).await {
                    error!("Error: no se pudo enviar mensaje a Data. {}", e);
                }
            }
        }
    }
}


