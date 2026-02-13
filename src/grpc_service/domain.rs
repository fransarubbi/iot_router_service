//! # Implementación de Servicios gRPC
//!
//! Este módulo contiene la implementación concreta de los traits generados por `tonic` (Protobuf).
//! Cada estructura aquí (`EdgeServiceImpl`, `ManagerServiceImpl`, `DataServiceImpl`) actúa como
//! un **Manejador de Conexiones**.
//!
//! ## Responsabilidades
//! 1. Aceptar conexiones entrantes TCP/HTTP2.
//! 2. Crear canales de comunicación bidireccionales (`mpsc`).
//! 3. Registrar al cliente en la [`RoutingTable`] para que pueda recibir mensajes.
//! 4. Lanzar una tarea asíncrona (`tokio::spawn`) para escuchar los mensajes entrantes y redirigirlos al Dispatcher.


use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use crate::grpc::edge_service_server::EdgeService;
use crate::grpc::{FromDataSaver, FromEdge, FromManager, ToDataSaver, ToEdge, ToManager};
use crate::grpc::data_service_server::DataService;
use crate::grpc::manager_service_server::ManagerService;
use crate::grpc_service::logic::{process_from_data, process_from_edge, process_from_manager};
use crate::router::domain::{RouterMessage, RoutingTable};


// ════════════════════════════════════════════════════════════════════
// Servicio Edge (Raspberry Pi)
// ════════════════════════════════════════════════════════════════════

/// Implementación del servicio gRPC para dispositivos Edge.
/// Maneja miles de conexiones simultáneas de Raspberrys.
pub struct EdgeServiceImpl {
    /// Referencia compartida a la tabla de enrutamiento global.
    pub routing_table: Arc<RoutingTable>,
    /// Canal para enviar mensajes al despachador central del Router.
    pub central_tx: mpsc::Sender<RouterMessage>,
}


impl EdgeServiceImpl {
    pub fn new(routing_table: Arc<RoutingTable>, central_tx: mpsc::Sender<RouterMessage>) -> Self {
        Self{
            routing_table,
            central_tx,
        }
    }
}


#[tonic::async_trait]
impl EdgeService for EdgeServiceImpl {
    // Definimos el tipo de stream de salida: Un ReceiverStream que envuelve resultados.
    type ConnectStreamStream = ReceiverStream<Result<ToEdge, Status>>;

    /// Maneja el handshake y el streaming bidireccional con un Edge.
    async fn connect_stream(&self, 
                            request: Request<Streaming<FromEdge>>,
    ) -> Result<Response<Self::ConnectStreamStream>, Status> {

        info!("Info: nueva conexión edge");

        let mut inbound_stream = request.into_inner();
        let (tx_to_edge, rx_to_edge) = mpsc::channel::<Result<ToEdge, Status>>(100);

        let central_tx = self.central_tx.clone();
        let routing_table = self.routing_table.clone();
        let tx_to_edge_clone = tx_to_edge.clone();

        tokio::spawn(async move {
            let mut connected_edge_id: Option<String> = None;
            while let Some(result) = inbound_stream.next().await {
                match result {
                    Ok(msg) => {
                        let edge_id = msg.edge_id.clone();

                        if edge_id.is_empty() {
                            warn!("Warning: mensaje sin edge_id");
                            continue;
                        }
                        connected_edge_id = Some(edge_id.clone());
                        let is_new = routing_table
                            .ensure_edge_registered(&edge_id, tx_to_edge_clone.clone()).await;

                        if is_new {
                            info!("Info: nuevo edge con id: {}", edge_id);
                        }
                        
                        process_from_edge(msg, &central_tx).await;
                    }
                    Err(e) => {
                        error!("Error: fallo la recepción del mensaje desde edge. {}", e);
                        break;
                    }
                }
            }

            if let Some(id) = connected_edge_id {
                info!("Info: conexión cerrada para edge: {}", id);
                routing_table.unregister_edge(&id).await;
            } else {
                info!("Info: conexión anónima cerrada");
            }
        });

        let outbound_stream = ReceiverStream::new(rx_to_edge);
        Ok(Response::new(outbound_stream))
    }
}


// ════════════════════════════════════════════════════════════════════
// Servicio Manager
// ════════════════════════════════════════════════════════════════════

/// Implementación del servicio gRPC para el Manager.
/// Solo hay una conexión de este tipo activa a la vez.
pub struct ManagerServiceImpl {
    pub routing_table: Arc<RoutingTable>,
    pub central_tx: mpsc::Sender<RouterMessage>,
}


impl ManagerServiceImpl {
    pub fn new(routing_table: Arc<RoutingTable>, central_tx: mpsc::Sender<RouterMessage>) -> Self {
        Self{
            routing_table,
            central_tx,
        }
    }
}


#[tonic::async_trait]
impl ManagerService for ManagerServiceImpl {
    type ConnectStreamStream = ReceiverStream<Result<ToManager, Status>>;

    async fn connect_stream(&self, 
                            request: Request<Streaming<FromManager>>,
    ) -> Result<Response<Self::ConnectStreamStream>, Status> {

        info!("Info: manager conectado");

        let mut inbound_stream = request.into_inner();
        let (tx_to_manager, rx_to_manager) = mpsc::channel::<Result<ToManager, Status>>(100);

        self.routing_table.register_manager(tx_to_manager).await;

        let central_tx = self.central_tx.clone();
        let routing_table = self.routing_table.clone();

        tokio::spawn(async move {
            while let Some(result) = inbound_stream.next().await {
                match result {
                    Ok(msg) => {
                        process_from_manager(msg, &central_tx).await;
                    }
                    Err(e) => {
                        error!("Error: fallo la recepción del mensaje desde manager. {}", e);
                        break;
                    }
                }
            }
            info!("Info: manager desconectado");
            routing_table.unregister_manager().await;
        });

        let outbound_stream = ReceiverStream::new(rx_to_manager);
        Ok(Response::new(outbound_stream))
    }
}


// ════════════════════════════════════════════════════════════════════
// Servicio Data (Python Data Science)
// ════════════════════════════════════════════════════════════════════

/// Implementación del servicio gRPC para el ingestor de datos (Data Saver).
/// Recibe el flujo masivo de telemetría.
pub struct DataServiceImpl {
    pub routing_table: Arc<RoutingTable>,
    pub central_tx: mpsc::Sender<RouterMessage>,
}


impl DataServiceImpl {
    pub fn new(routing_table: Arc<RoutingTable>, central_tx: mpsc::Sender<RouterMessage>) -> Self {
        Self{
            routing_table,
            central_tx,
        }
    }
}


#[tonic::async_trait]
impl DataService for DataServiceImpl {
    type ConnectStreamStream = ReceiverStream<Result<ToDataSaver, Status>>;

    async fn connect_stream(&self, 
                            request: Request<Streaming<FromDataSaver>>,
    ) -> Result<Response<Self::ConnectStreamStream>, Status> {

        info!("Info: data conectado");

        let mut inbound_stream = request.into_inner();
        let (tx_to_data, rx_to_data) = mpsc::channel::<Result<ToDataSaver, Status>>(100);

        self.routing_table.register_data(tx_to_data).await;

        let central_tx = self.central_tx.clone();
        let routing_table = self.routing_table.clone();

        tokio::spawn(async move {
            while let Some(result) = inbound_stream.next().await {
                match result {
                    Ok(msg) => {
                        process_from_data(msg, &central_tx).await;
                    }
                    Err(e) => {
                        error!("Error: fallo recepción de mensaje desde data. {}", e);
                        break;
                    }
                }
            }

            info!("Info: data desconectado");
            routing_table.unregister_data().await;
        });

        let outbound_stream = ReceiverStream::new(rx_to_data);
        Ok(Response::new(outbound_stream))
    }
}