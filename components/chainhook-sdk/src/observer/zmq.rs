use chainhook_types::BitcoinBlockSignaling;
use hiro_system_kit::slog;
use std::sync::mpsc::Sender;
use zmq::Socket;

use crate::{
    indexer::{
        bitcoin::{build_http_client, download_and_parse_block_with_retry},
        fork_scratch_pad::ForkScratchPad,
    },
    utils::Context,
};
use std::collections::VecDeque;

use super::{EventObserverConfig, ObserverCommand};

pub struct ConfigZmqSocket<'a> {
    /// Topics to subscribe to
    subscribe_topics: &'a [&'a str],
    /// Receive high water mark (0 for unlimited)
    receive_hwm: i32,
    /// Enable TCP keepalive 1 to enable, 0 to disable
    tcp_keepalive: i32,
    /// Start sending keepalive probes after this number of seconds
    tcp_keepalive_idle: i32,
    /// Send a keepalive probe every this number of seconds
    tcp_keepalive_intvl: i32,
    /// Number of keepalive probes to send before considering the connection dead
    tcp_keepalive_cnt: i32,
}

pub const DEFAULT_CONFIG_ZMQ_SOCKET: ConfigZmqSocket = ConfigZmqSocket {
    subscribe_topics: &["hashblock"],
    receive_hwm: 0,
    tcp_keepalive: 1,
    // 5 minutes of waiting for keepalive response
    tcp_keepalive_idle: 300,
    // 2 hours of sending keepalive probes
    tcp_keepalive_intvl: 60,
    // 120 times
    tcp_keepalive_cnt: 120,
};

pub const UPDATED_CONFIG_ZMQ_SOCKET: ConfigZmqSocket = ConfigZmqSocket {
    subscribe_topics: &["hashblock"],
    receive_hwm: 0,
    tcp_keepalive: 1,
    // 10 minutes of waiting for keepalive response
    // 10 minutes is approximation of BTC block mining time
    tcp_keepalive_idle: 600,
    // 5 minutes of sending keepalive probes
    tcp_keepalive_intvl: 20,
    tcp_keepalive_cnt: 15,
};

fn new_zmq_socket(config: &ConfigZmqSocket) -> Socket {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SUB).unwrap();
    for topic in config.subscribe_topics {
        assert!(socket.set_subscribe(topic.as_bytes()).is_ok());
    }
    assert!(socket.set_rcvhwm(config.receive_hwm).is_ok());
    // We override the OS default behavior:
    assert!(socket.set_tcp_keepalive(config.tcp_keepalive).is_ok());
    assert!(socket
        .set_tcp_keepalive_idle(config.tcp_keepalive_idle)
        .is_ok());
    assert!(socket
        .set_tcp_keepalive_intvl(config.tcp_keepalive_intvl)
        .is_ok());
    assert!(socket
        .set_tcp_keepalive_cnt(config.tcp_keepalive_cnt)
        .is_ok());
    socket
}

pub async fn start_zeromq_runloop(
    config: &EventObserverConfig,
    observer_commands_tx: Sender<ObserverCommand>,
    ctx: &Context,
) {
    let BitcoinBlockSignaling::ZeroMQ(ref bitcoind_zmq_url) = config.bitcoin_block_signaling else {
        unreachable!()
    };

    let bitcoind_zmq_url = bitcoind_zmq_url.clone();
    let bitcoin_config = config.get_bitcoin_config();
    let http_client = build_http_client();

    ctx.try_log(|logger| {
        slog::info!(
            logger,
            "Waiting for ZMQ connection acknowledgment from bitcoind"
        )
    });

    let mut socket = new_zmq_socket(&UPDATED_CONFIG_ZMQ_SOCKET);
    assert!(socket.connect(&bitcoind_zmq_url).is_ok());
    ctx.try_log(|logger| slog::info!(logger, "Waiting for ZMQ messages from bitcoind"));

    let mut bitcoin_blocks_pool = ForkScratchPad::new();

    loop {
        let msg = match socket.recv_multipart(0) {
            Ok(msg) => msg,
            Err(e) => {
                ctx.try_log(|logger| {
                    slog::error!(logger, "Unable to receive ZMQ message: {}", e.to_string())
                });
                socket = new_zmq_socket(&UPDATED_CONFIG_ZMQ_SOCKET);
                assert!(socket.connect(&bitcoind_zmq_url).is_ok());
                continue;
            }
        };
        let (topic, data, _sequence) = (&msg[0], &msg[1], &msg[2]);

        if !topic.eq(b"hashblock") {
            ctx.try_log(|logger| slog::error!(logger, "Topic not supported",));
            continue;
        }

        let block_hash = hex::encode(data);

        ctx.try_log(|logger| slog::info!(logger, "Bitcoin block hash announced #{block_hash}",));

        let mut block_hashes: VecDeque<String> = VecDeque::new();
        block_hashes.push_front(block_hash);

        while let Some(block_hash) = block_hashes.pop_front() {
            let block = match download_and_parse_block_with_retry(
                &http_client,
                &block_hash,
                &bitcoin_config,
                ctx,
            )
            .await
            {
                Ok(block) => block,
                Err(e) => {
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "unable to download_and_parse_block: {}",
                            e.to_string()
                        )
                    });
                    continue;
                }
            };

            let header = block.get_block_header();
            ctx.try_log(|logger| {
                slog::info!(
                    logger,
                    "Bitcoin block #{} dispatched for processing",
                    block.height
                )
            });

            let _ = observer_commands_tx.send(ObserverCommand::ProcessBitcoinBlock(block));

            if bitcoin_blocks_pool.can_process_header(&header) {
                match bitcoin_blocks_pool.process_header(header, ctx) {
                    Ok(Some(event)) => {
                        let _ = observer_commands_tx
                            .send(ObserverCommand::PropagateBitcoinChainEvent(event));
                    }
                    Err(e) => {
                        ctx.try_log(|logger| {
                            slog::warn!(logger, "Unable to append block: {:?}", e)
                        });
                    }
                    Ok(None) => {
                        ctx.try_log(|logger| slog::warn!(logger, "Unable to append block"));
                    }
                }
            } else {
                // Handle a behaviour specific to ZMQ usage in bitcoind.
                // Considering a simple re-org:
                // A (1) - B1 (2) - C1 (3)
                //       \ B2 (4) - C2 (5) - D2 (6)
                // When D2 is being discovered (making A -> B2 -> C2 -> D2 the new canonical fork)
                // it looks like ZMQ is only publishing D2.
                // Without additional operation, we end up with a block that we can't append.
                let parent_block_hash = header
                    .parent_block_identifier
                    .get_hash_bytes_str()
                    .to_string();
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Possible re-org detected, retrieving parent block {parent_block_hash}"
                    )
                });
                block_hashes.push_front(block_hash);
                block_hashes.push_front(parent_block_hash);
            }
        }
    }
}
