//! Configuración mTLS (Mutual TLS) de grado enterprise para el servidor.
//!
//! Este módulo proporciona la lógica criptográfica necesaria para establecer
//! un servidor HTTPS seguro que no solo encripta el tráfico, sino que exige
//! autenticación bidireccional.
//!
//! A diferencia del TLS tradicional (donde solo el servidor prueba su identidad),
//! aquí se implementa un verificador que obliga a cada dispositivo (Edge/Cliente)
//! a presentar un certificado criptográfico válido y firmado por una Autoridad
//! Certificante (CA) interna de la organización.


use anyhow::{Context, Result};
use rustls::{
    server::{WebPkiClientVerifier},
    ServerConfig, RootCertStore,
};
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::{fs::File, io::BufReader, sync::Arc};
use rustls::server::danger::ClientCertVerifier;
use tracing::info;


/// Construye la configuración mTLS completa para el servidor HTTPS.
///
/// Esta función lee los archivos PEM del disco, extrae los certificados y las claves
/// privadas, e inicializa el motor de `rustls`. Configura el servidor para que
/// requiera y valide obligatoriamente los certificados de los clientes contra una
/// CA raíz de confianza.
///
/// # Argumentos
///
/// * `cert_path` - Ruta en el sistema de archivos al certificado público del servidor (PEM).
/// * `key_path` - Ruta a la clave privada del servidor. **Debe estar en formato PKCS#8**.
/// * `client_ca_path` - Ruta al certificado de la Autoridad Certificante (CA) que
///   se utilizará para validar a los clientes entrantes.
///
/// # Retorna
///
/// Retorna un `Arc<ServerConfig>` listo para ser inyectado en servidores compatibles
/// con `rustls` (como `axum-server` o `tonic`). Se envuelve en un `Arc` porque la
/// configuración suele compartirse entre múltiples hilos trabajadores.
///
/// # Errores
///
/// Retorna un `anyhow::Error` detallado si ocurre alguno de los siguientes problemas:
/// * Alguno de los archivos especificados no existe o no tiene permisos de lectura.
/// * Los archivos no contienen formato PEM válido.
/// * El archivo de clave privada no contiene ninguna clave en formato PKCS#8.
/// * Falla la construcción del verificador de la cadena de confianza (WebPKI).
pub fn build_mtls_config(cert_path: &str,
                         key_path: &str,
                         client_ca_path: &str,
) -> Result<Arc<ServerConfig>> {

    // Cargar certificado del servidor
    let cert_file = File::open(cert_path)
        .with_context(|| format!("Error: no se puede abrir cert: {cert_path}"))?;

    let server_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        certs(&mut BufReader::new(cert_file))
            .collect::<Result<_, _>>()
            .context("Error: no se pudo leer certificados del servidor")?;

    // Cargar clave privada del servidor
    let key_file = File::open(key_path)
        .with_context(|| format!("Error: no se puede abrir key: {key_path}"))?;

    let mut keys: Vec<rustls::pki_types::PrivateKeyDer<'static>> =
        pkcs8_private_keys(&mut BufReader::new(key_file))
            .map(|k| k.map(rustls::pki_types::PrivateKeyDer::Pkcs8))
            .collect::<Result<_, _>>()
            .context("Error: no se pudo leer la clave privada")?;

    if keys.is_empty() {
        anyhow::bail!("Error: no se encontró ninguna clave PKCS8 en {key_path}");
    }

    // Cargar CA raíz para verificar clientes (la parte MUTUAL de mTLS)
    let ca_file = File::open(client_ca_path)
        .with_context(|| format!("Error: no se puede abrir CA: {client_ca_path}"))?;

    let ca_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        certs(&mut BufReader::new(ca_file))
            .collect::<Result<_, _>>()
            .context("Error: no se pudo leer CA de clientes")?;

    // Construimos el almacén de confianza (Trust Store) con la CA
    let mut root_store = RootCertStore::empty();
    for ca_cert in ca_certs {
        root_store
            .add(ca_cert)
            .context("Error: no se pudo añadir CA al root store")?;
    }

    // Construir el verificador de clientes (el núcleo del mTLS)
    // Este componente se encargará de interceptar el handshake TLS y rechazar
    // cualquier conexión que no presente un certificado firmado por `root_store`.
    let client_verifier: Arc<dyn ClientCertVerifier> =
        WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .context("Error: no se pudo construir WebPkiClientVerifier")?;

    // Ensamblar ServerConfig
    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        // Extraemos la primera clave privada de la lista obtenida en el paso 2
        .with_single_cert(server_certs, keys.remove(0))
        .context("Error: no se pudo configurar el certificado del servidor")?;

    info!("Info: configuración mTLS construida correctamente");
    Ok(Arc::new(config))
}