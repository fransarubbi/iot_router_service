//! # Lógica de Procesamiento de Mensajes gRPC
//!
//! Este módulo actúa como una **Capa de Adaptación** entre la red (gRPC) y el sistema de enrutamiento interno.
//!
//! Su responsabilidad es:
//! 1. Recibir los mensajes crudos definidos en el Protocol Buffer (`FromEdge`, `FromManager`, etc.).
//! 2. Desempaquetar el contenido (`payload`).
//! 3. Envolver el contenido en el mensaje interno del sistema (`RouterMessage`).
//! 4. Enviarlo al canal central (`central_tx`) para que el [`Dispatcher`] decida qué hacer.


use tokio::sync::{mpsc};
use tracing::{debug, error};
use crate::grpc::{
    // Mensajes
    ToEdge, FromEdge, to_edge,
    ToManager, FromManager, to_manager, from_manager,
    ToDataSaver, FromDataSaver, to_data_saver,
};
use crate::router::domain::{RouterMessage};


/// **Procesa mensajes provenientes de un dispositivo Edge.**
///
/// Clasifica el mensaje según su tipo y lo dirige al destino correcto:
/// * Datos de sensores (`Measurement`, `Monitor`, `Alerts`) -> Se envían a **Data Saver**.
/// * Datos de control (`Settings`, `Firmware`) -> Se envían al **Manager**.
///
/// # Argumentos
/// * `msg`: El mensaje `FromEdge` recibido directamente del stream gRPC.
/// * `central_tx`: El canal de envío hacia el Dispatcher del Router.
pub async fn process_from_edge(msg: FromEdge, central_tx: &mpsc::Sender<RouterMessage>) {
    use crate::grpc::from_edge::Payload;

    if let Some(payload) = msg.payload {
        match payload {
            // Mensajes que van a data
            Payload::Measurement(measurement) => {
                debug!("mensaje Measurement recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::Measurement(measurement)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::Monitor(monitor) => {
                debug!("mensaje Monitor recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::Monitor(monitor)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::AlertAir(alert) => {
                debug!("mensaje AlertAir recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::AlertAir(alert)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::AlertTh(alert) => {
                debug!("mensaje AlertTh recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::AlertTh(alert)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::MeasurementBatch(measurement) => {
                debug!("mensaje MeasurementBatch recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::MeasurementBatch(measurement)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::MonitorBatch(monitor) => {
                debug!("mensaje MonitorBatch recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::MonitorBatch(monitor)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::AlertAirBatch(alert) => {
                debug!("mensaje AlertAirBatch recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::AlertAirBatch(alert)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::AlertThBatch(alert) => {
                debug!("mensaje AlertThBatch recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::AlertThBatch(alert)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::Metric(metric) => {
                debug!("mensaje Metric recibido desde edge");
                let router_msg = RouterMessage::ToData {
                    message: ToDataSaver {
                        payload: Some(to_data_saver::Payload::Metric(metric)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },

            // Mensajes que van a Manager
            Payload::Settings(settings) => {
                debug!("mensaje Settings recibido desde edge");
                let router_msg = RouterMessage::ToManager {
                    message: ToManager {
                        edge_id: msg.edge_id.clone(),
                        payload: Some(to_manager::Payload::Settings(settings)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::SettingOk(setting_ok) => {
                debug!("mensaje SettingsOk recibido desde edge");
                let router_msg = RouterMessage::ToManager {
                    message: ToManager {
                        edge_id: msg.edge_id.clone(),
                        payload: Some(to_manager::Payload::SettingOk(setting_ok)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::FirmwareOutcome(outcome) => {
                debug!("mensaje FirmwareOutcome recibido desde edge");
                let router_msg = RouterMessage::ToManager {
                    message: ToManager {
                        edge_id: msg.edge_id.clone(),
                        payload: Some(to_manager::Payload::FirmwareOutcome(outcome)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
            Payload::HelloWorld(hello) => {
                debug!("mensaje HelloWorld recibido desde edge");
                let router_msg = RouterMessage::ToManager {
                    message: ToManager {
                        edge_id: msg.edge_id.clone(),
                        payload: Some(to_manager::Payload::HelloWorld(hello)),
                    },
                };
                if central_tx.send(router_msg).await.is_err() {
                    error!("Error: no se pudo enviar mensaje a traves de central_tx");
                }
            },
        }
    }
}


/// **Procesa mensajes provenientes del Manager.**
///
/// Convierte comandos administrativos (`FromManager`) en mensajes ejecutables para el Edge (`ToEdge`).
/// El `edge_id` destino viene especificado dentro del mensaje del Manager.
pub async fn process_from_manager(msg: FromManager, central_tx: &mpsc::Sender<RouterMessage>) {
    let edge_id = msg.edge_id.clone();

    if let Some(payload) = msg.payload {
        use from_manager::Payload;
        let to_edge_payload = match payload {
            Payload::SettingOk(s) => {
                debug!("mensaje SettingOk recibido desde manager");
                Some(to_edge::Payload::SettingOk(s))
            },
            Payload::UpdateFirmware(u) => {
                debug!("mensaje UpdateFirmware recibido desde manager");
                Some(to_edge::Payload::UpdateFirmware(u))
            },
            Payload::Network(n) => {
                debug!("mensaje Network recibido desde manager");
                Some(to_edge::Payload::Network(n))
            },
            Payload::DeleteHub(d) => {
                debug!("mensaje DeleteHub recibido desde manager");
                Some(to_edge::Payload::DeleteHub(d))
            },
            Payload::Settings(s) => {
                debug!("mensaje Settings recibido desde manager");
                Some(to_edge::Payload::Settings(s))
            },
        };

        if let Some(payload) = to_edge_payload {
            let msg = RouterMessage::ToEdge {
                destination_edge_id: edge_id.clone(),
                message: ToEdge {
                    edge_id: edge_id.clone(),
                    payload: Some(payload),
                },
            };

            if central_tx.send(msg).await.is_err() {
                error!("Error: no se pudo enviar mensaje a traves de central_tx");
            }
        }
    }
}


/// **Procesa mensajes provenientes de Data Saver.**
///
/// Usado para señales de control global, como Heartbeats.
/// Si se recibe un Heartbeat, se empaqueta para ser enviado a "todos" (Broadcast).
pub async fn process_from_data(msg: FromDataSaver,
                               central_tx: &mpsc::Sender<RouterMessage>) {

    use crate::grpc::from_data_saver::Payload;

    if let Some(payload) = msg.payload {
        debug!("mensaje Heartbeat desde data");
        let to_edge_payload = match payload {
            Payload::Heartbeat(heartbeat) => {
                Some(to_edge::Payload::Heartbeat(heartbeat))
            }
        };

        if let Some(payload) = to_edge_payload {
            let to_edge = RouterMessage::ToEdge {
                destination_edge_id: "all".to_string(),
                message: ToEdge {
                    edge_id: "all".to_string(),
                    payload: Some(payload.clone()),
                },
            };

            if central_tx.send(to_edge).await.is_err() {
                error!("Error: no se pudo enviar broadcast heartbeat");
            }
        }
    }
}
