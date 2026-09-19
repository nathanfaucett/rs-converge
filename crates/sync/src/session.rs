use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use engine::{Checkpoint, Engine, EngineError, Kernel, RowCodec};
use thiserror::Error;

use crate::{PROTOCOL_VERSION, SyncHello, SyncMessage, SyncTransport};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRole {
    Initiator,
    Responder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionConfig {
    pub max_envelopes_per_frame: usize,
    pub checkpoint_threshold: Option<usize>,
}

impl SessionConfig {
    pub const fn new() -> Self {
        Self {
            max_envelopes_per_frame: 64,
            checkpoint_threshold: None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncResult {
    pub received_envelopes: usize,
    pub sent_envelopes: usize,
}

#[derive(Debug, Error)]
pub enum SyncError<E> {
    #[error("engine error: {0}")]
    Engine(#[from] EngineError),

    #[error("transport error: {0}")]
    Transport(E),

    #[error("invalid session configuration")]
    InvalidConfiguration,

    #[error("protocol codec error: {0}")]
    Protocol(String),

    #[error("incompatible protocol version: {0}")]
    IncompatibleProtocol(u16),

    #[error("unexpected sync message")]
    UnexpectedMessage,
}

pub async fn synchronize<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
) -> Result<SyncResult, SyncError<T::Error>>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    if config.max_envelopes_per_frame == 0 {
        return Err(SyncError::InvalidConfiguration);
    }

    match role {
        SyncRole::Initiator => {
            send_hello(engine, transport).await?;
            receive_hello(transport).await?;
        }
        SyncRole::Responder => {
            receive_hello(transport).await?;
            send_hello(engine, transport).await?;
        }
    }

    let mut result = SyncResult::default();
    let mut first_round = true;
    loop {
        let (sent, received) = sync_round(engine, transport, config, role, first_round).await?;
        first_round = false;
        result.sent_envelopes += sent;
        result.received_envelopes += received;
        if sent == 0 && received == 0 {
            return Ok(result);
        }
    }
}

async fn send_hello<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
) -> Result<(), SyncError<T::Error>>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let hello = SyncHello {
        protocol_version: PROTOCOL_VERSION,
        frontier: engine.frontier().await?,
    };
    send_message(transport, &SyncMessage::Hello(hello)).await
}

async fn receive_hello<T>(transport: &mut T) -> Result<(), SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let SyncMessage::Hello(hello) = receive_message(transport).await? else {
        return Err(SyncError::UnexpectedMessage);
    };
    if hello.protocol_version != PROTOCOL_VERSION {
        return Err(SyncError::IncompatibleProtocol(hello.protocol_version));
    }
    Ok(())
}

async fn sync_round<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    config: &SessionConfig,
    role: SyncRole,
    first_round: bool,
) -> Result<(usize, usize), SyncError<T::Error>>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let remote_frontier = match role {
        SyncRole::Initiator => {
            send_message(transport, &SyncMessage::Frontier(engine.frontier().await?)).await?;
            receive_frontier(transport).await?
        }
        SyncRole::Responder => {
            let remote_frontier = receive_frontier(transport).await?;
            send_message(transport, &SyncMessage::Frontier(engine.frontier().await?)).await?;
            remote_frontier
        }
    };
    let envelopes = engine.missing_envelopes(&remote_frontier).await?;
    let send_checkpoint = first_round
        && remote_frontier.heads.is_empty()
        && config
            .checkpoint_threshold
            .is_some_and(|threshold| envelopes.len() > threshold);

    match role {
        SyncRole::Initiator => {
            let sent = send_transfer(
                engine,
                transport,
                envelopes,
                config.max_envelopes_per_frame,
                send_checkpoint,
            )
            .await?;
            let received = receive_envelopes(engine, transport, first_round).await?;
            Ok((sent, received))
        }
        SyncRole::Responder => {
            let received = receive_envelopes(engine, transport, first_round).await?;
            let sent = send_transfer(
                engine,
                transport,
                envelopes,
                config.max_envelopes_per_frame,
                send_checkpoint,
            )
            .await?;
            Ok((sent, received))
        }
    }
}

async fn receive_frontier<T>(transport: &mut T) -> Result<engine::Frontier, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let SyncMessage::Frontier(frontier) = receive_message(transport).await? else {
        return Err(SyncError::UnexpectedMessage);
    };
    Ok(frontier)
}

async fn send_transfer<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    envelopes: Vec<engine::TransactionEnvelope>,
    batch_size: usize,
    send_checkpoint: bool,
) -> Result<usize, SyncError<T::Error>>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let count = envelopes.len();
    if send_checkpoint {
        let checkpoint = engine.export_checkpoint().await?.encode()?;
        send_message(transport, &SyncMessage::Checkpoint(checkpoint)).await?;
    }
    for batch in envelopes.chunks(batch_size) {
        let bytes = batch
            .iter()
            .map(engine::TransactionEnvelope::encode)
            .collect::<Result<Vec<_>, _>>()?;
        send_message(transport, &SyncMessage::Envelopes(bytes)).await?;
    }
    send_message(transport, &SyncMessage::Done).await?;
    Ok(count)
}

async fn receive_envelopes<K, R, T>(
    engine: &Engine<K, R>,
    transport: &mut T,
    allow_checkpoint: bool,
) -> Result<usize, SyncError<T::Error>>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let mut count = 0;
    let mut checkpoint_allowed = allow_checkpoint;
    loop {
        match receive_message(transport).await? {
            SyncMessage::Checkpoint(bytes) if checkpoint_allowed => {
                checkpoint_allowed = false;
                let checkpoint = Checkpoint::decode(&bytes)
                    .map_err(|error| SyncError::Protocol(error.to_string()))?;
                engine.import_checkpoint(checkpoint).await?;
            }
            SyncMessage::Envelopes(envelopes) => {
                checkpoint_allowed = false;
                count += envelopes.len();
                for envelope in envelopes {
                    let _ = engine.import_envelope_bytes(envelope).await?;
                }
            }
            SyncMessage::Done => return Ok(count),
            SyncMessage::Hello(_) | SyncMessage::Frontier(_) | SyncMessage::Checkpoint(_) => {
                return Err(SyncError::UnexpectedMessage);
            }
        }
    }
}

async fn send_message<T>(
    transport: &mut T,
    message: &SyncMessage,
) -> Result<(), SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let frame =
        postcard::to_allocvec(message).map_err(|error| SyncError::Protocol(error.to_string()))?;
    transport.send(frame).await.map_err(SyncError::Transport)
}

async fn receive_message<T>(transport: &mut T) -> Result<SyncMessage, SyncError<T::Error>>
where
    T: SyncTransport,
    T::Error: core::fmt::Display,
{
    let frame = transport.receive().await.map_err(SyncError::Transport)?;
    postcard::from_bytes(&frame).map_err(|error| SyncError::Protocol(error.to_string()))
}
