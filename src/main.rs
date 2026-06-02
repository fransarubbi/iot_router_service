mod grpc_service;
mod router;
mod system;
mod config;
mod https;
mod metrics;
mod connections;
mod mtls;
mod error;
mod middleware;
mod message;

pub mod grpc {
    tonic::include_proto!("grpc");
}


use tokio::sync::mpsc;
use tracing::info;
use crate::grpc_service::domain::{DataServiceImpl, EdgeServiceImpl, ManagerServiceImpl};
use crate::https::domain::HttpsService;
use crate::router::domain::{dispatcher_task, RouterMessage, RoutingTable};
use crate::system::domain::{init_server, init_tracing, System};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let _ = rustls::crypto::ring::default_provider().install_default();

    let system = System::new()?;
    init_tracing(&system);

    info!("Info: router gRPC iniciando");

    let routing_table = RoutingTable::new();
    let (central_tx, central_rx) = mpsc::channel::<RouterMessage>(1000);

    // Dispatcher
    let rt_clone = routing_table.clone();
    tokio::spawn(async move {
        dispatcher_task(central_rx, rt_clone).await;
    });

    let edge_service = EdgeServiceImpl::new(routing_table.clone(), central_tx.clone());
    let manager_service = ManagerServiceImpl::new(routing_table.clone(), central_tx.clone());
    let data_service = DataServiceImpl::new(routing_table.clone(), central_tx.clone());
    let https_service = HttpsService::new(central_tx.clone());

    init_server(edge_service,
                manager_service,
                data_service,
                https_service,
                &system).await?;

    Ok(())
}
