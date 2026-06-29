//! # Configuración y Arranque del Sistema
//!
//! Este módulo se encarga de:
//! 1. Cargar la configuración desde variables de entorno o archivos `.env`.
//! 2. Inicializar el sistema de Trazabilidad (Logging estructurado).
//! 3. Configurar y levantar los servidores gRPC (Edge y Manager) con sus respectivas políticas de seguridad.

use crate::config::certs::{CA_ROUTER, CRT_ROUTER, KEY_ROUTER};
use crate::grpc::data_service_server::DataServiceServer;
use crate::grpc::edge_service_server::EdgeServiceServer;
use crate::grpc::manager_service_server::ManagerServiceServer;
use crate::grpc_service::domain::{DataServiceImpl, EdgeServiceImpl, ManagerServiceImpl};
use crate::https::domain::HttpsService;
use crate::metrics::logic::init_global;
use axum_server::Server as AxumServer;
use std::{env, fs};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug)]
pub struct System {
    /// Host donde escuchará o se conectará el servicio gRPC.
    /// Por defecto: `localhost` (o `0.0.0.0` en contenedores).
    pub grpc_host: String,

    /// Puerto seguro (mTLS) para la conexión con dispositivos Edge.
    /// Por defecto: `50051`.
    pub grpc_port_edge: u16,

    /// Puerto interno (TLS opcional/Texto plano) para APIs de gestión y datos.
    /// Por defecto: `50052`.
    pub grpc_port_server: u16,

    /// Host donde se conectará el servicio HTTPS.
    pub https_host: String,

    /// Puerto para la conexión.
    pub https_port: u16,

    /// Entorno de ejecución actual (`development`, `staging`, `production`).
    /// Controla el formato de logs y la carga de configuraciones.
    pub environment: String,

    /// Nivel de detalle de los logs (ej. `info`, `debug`, `warn`).
    pub rust_log: String,
}

impl System {
    /// Constructor que carga la configuración del entorno.
    ///
    /// # Errores
    /// Retorna error si los puertos configurados no son números válidos.
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        info!("Info: creando objeto system");

        let environment = env::var("ENVIRONMENT").unwrap_or_else(|_| "development".into());

        if environment == "development" {
            dotenv::dotenv().ok();
        }

        Ok(System {
            grpc_host: env::var("GRPC_HOST").unwrap_or("localhost".to_string()),

            grpc_port_edge: env::var("GRPC_PORT_EDGE")
                .unwrap_or("50051".to_string())
                .parse()
                .expect("GRPC_PORT_EDGE debe ser un número"),

            grpc_port_server: env::var("GRPC_PORT_SERVER")
                .unwrap_or("50052".to_string())
                .parse()
                .expect("GRPC_PORT_SERVER debe ser un número"),

            https_host: env::var("HTTPS_HOST").unwrap_or("0.0.0.0".to_string()),

            https_port: env::var("HTTPS_PORT")
                .unwrap_or("8080".to_string())
                .parse()
                .expect("HTTPS_PORT debe ser un número"),

            rust_log: env::var("RUST_LOG").unwrap_or_else(|_| match environment.as_str() {
                "development" => "debug".to_string(),
                "staging" => "info".to_string(),
                _ => "warn".to_string(),
            }),

            environment,
        })
    }
}

/// Inicializa el sistema de trazabilidad y logs (Tracing).
///
/// Configura el formato de salida basándose en el entorno:
/// * **Production**: Salida JSON (para logs estructurados en la nube).
/// * **Development/Otros**: Salida "Pretty" (colores y formato legible).
///
/// # Argumentos
/// * `system`: Referencia a la configuración cargada para leer el nivel de log (`rust_log`).
pub fn init_tracing(system: &System) {
    let filter = EnvFilter::try_new(&system.rust_log).unwrap_or_else(|_| EnvFilter::new("info"));

    let builder = fmt().pretty().with_env_filter(filter).with_target(false);

    if system.environment == "production" {
        builder.json().init();
    } else {
        builder.pretty().init();
    }
}

/// Inicializa y ejecuta los servidores gRPC concurrentemente.
///
/// Implementa la estrategia de "Dual Server":
/// 1. **Servidor Edge (Puerto Seguro):** Requiere mTLS (Certificados de cliente).
/// 2. **Servidor Management (Puerto Interno):** API estándar para Java/Python.
///
/// # Argumentos
/// * `edge_service`: Implementación de la lógica para Raspberrys.
/// * `manager_service`: Implementación de la lógica para el Manager.
/// * `data_service`: Implementación de la lógica para Data Saver.
/// * `system`: Configuración global (puertos, host).
pub async fn init_server(
    edge_service: EdgeServiceImpl,
    manager_service: ManagerServiceImpl,
    data_service: DataServiceImpl,
    https_service: HttpsService,
    system: &System,
) -> Result<(), Box<dyn std::error::Error>> {
    // ════════════════════════════════════════════════════════════════════
    // Configuración de Seguridad (mTLS)
    // ════════════════════════════════════════════════════════════════════

    // Cargamos la CA que valida a las Raspberrys
    let client_ca_pem = fs::read_to_string(CA_ROUTER)?;
    let client_ca_cert = Certificate::from_pem(client_ca_pem);

    // El certificado y llave privada de este Servidor (Router)
    let server_cert_pem = fs::read_to_string(CRT_ROUTER)?;
    let server_key_pem = fs::read_to_string(KEY_ROUTER)?;
    let server_identity = Identity::from_pem(server_cert_pem, server_key_pem);

    // Configuración mTLS: pedir certificado al cliente y validar con la CA
    let tls_config = ServerTlsConfig::new()
        .identity(server_identity)
        .client_ca_root(client_ca_cert);

    // ════════════════════════════════════════════════════════════════════
    // Definición de Direcciones
    // ════════════════════════════════════════════════════════════════════
    let addr_edge = format!("{}:{}", system.grpc_host, system.grpc_port_edge).parse()?;
    let addr_server = format!("{}:{}", system.grpc_host, system.grpc_port_server).parse()?;
    let addr_http = format!("{}:{}", system.https_host, system.https_port).parse()?;

    // ════════════════════════════════════════════════════════════════════
    // Lanzamiento de Servidores
    // ════════════════════════════════════════════════════════════════════
    let edge_server = async {
        info!("Info: servidor edge (mTLS) escuchando en {}", addr_edge);
        Server::builder()
            .tls_config(tls_config)?
            // Permite que el canal esté en silencio hasta 40 segundos antes de enviar un ping
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(40)))
            // Si envía el ping, espera 20 segundos la respuesta antes de cortar
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(20)))
            .add_service(
                EdgeServiceServer::new(edge_service)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
            .serve(addr_edge)
            .await
    };

    let mgmt_server = async {
        info!("Info: servidor apis escuchando en {}", addr_server);
        Server::builder()
            .add_service(
                ManagerServiceServer::new(manager_service)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
            .add_service(
                DataServiceServer::new(data_service)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
            .serve(addr_server)
            .await
    };

    init_global();

    https_service
        .registry
        .clone()
        .start_reaper(std::time::Duration::from_secs(10));
    let https_app = crate::https::domain::build_router(https_service.clone());

    let http_server = async {
        info!("Info: servidor HTTP (sin TLS) escuchando en {}", addr_http);
        AxumServer::bind(addr_http)
            .serve(https_app.into_make_service())
            .await
            .map_err(anyhow::Error::from)
    };

    info!("Info: iniciando todos los servicios gRPC y HTTPS...");

    tokio::try_join!(
        async { edge_server.await.map_err(anyhow::Error::from) },
        async { mgmt_server.await.map_err(anyhow::Error::from) },
        async { http_server.await.map_err(anyhow::Error::from) }
    )?;

    Ok(())
}
