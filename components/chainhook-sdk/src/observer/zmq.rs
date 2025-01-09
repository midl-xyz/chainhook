use chainhook_types::{BitcoinBlockSignaling, BlockHeader};
use hiro_system_kit::slog;
use reqwest::Client as HttpClient;
use std::sync::mpsc::Sender;
use zmq::Socket;

use crate::{
    indexer::{
        bitcoin::{
            build_http_client, download_and_parse_block_with_retry, get_block_count,
            get_block_count_with_retry, retrieve_block_hash_with_retry,
        },
        fork_scratch_pad::{ForkScratchPad, CONFIRMED_SEGMENT_MINIMUM_LENGTH},
    },
    utils::Context,
};
use std::collections::VecDeque;

use super::{BitcoinConfig, EventObserverConfig, ObserverCommand};

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
    initialize_recent_chain_state(
        &http_client,
        &bitcoin_config,
        &mut bitcoin_blocks_pool,
        &observer_commands_tx,
        ctx,
    )
    .await
    .unwrap();

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
                process_and_propagate_header(
                    header,
                    &mut bitcoin_blocks_pool,
                    &observer_commands_tx,
                    ctx,
                );
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

fn process_and_propagate_header(
    header: BlockHeader,
    bitcoin_blocks_pool: &mut ForkScratchPad,
    observer_commands_tx: &Sender<ObserverCommand>,
    ctx: &Context,
) {
    match bitcoin_blocks_pool.process_header(header, ctx) {
        Ok(Some(event)) => {
            let _ = observer_commands_tx.send(ObserverCommand::PropagateBitcoinChainEvent(event));
        }
        Err(e) => {
            ctx.try_log(|logger| slog::warn!(logger, "Unable to append block: {:?}", e));
        }
        Ok(None) => {
            ctx.try_log(|logger| slog::warn!(logger, "Unable to append block"));
        }
    }
}

async fn initialize_recent_chain_state(
    http_client: &HttpClient,
    bitcoin_config: &BitcoinConfig,
    fork_scratch_pad: &mut ForkScratchPad,
    observer_commands_tx: &Sender<ObserverCommand>,
    ctx: &Context,
) -> Result<(), String> {
    ctx.try_log(|logger| {
        slog::info!(
            logger,
            "Initializing bitcoin blocks pool with recent chain state",
        )
    });
    let tip_height = get_block_count_with_retry(http_client, bitcoin_config, ctx).await?;

    // Fetch last tip_height - CONFIRMED_SEGMENT_MINIMUM_LENGTH to tip_height blocks
    for height in (tip_height.wrapping_sub(CONFIRMED_SEGMENT_MINIMUM_LENGTH as u64))..=tip_height {
        let current_hash =
            retrieve_block_hash_with_retry(http_client, &height, bitcoin_config, ctx).await?;
        if let Ok(block) =
            download_and_parse_block_with_retry(http_client, &current_hash, bitcoin_config, ctx)
                .await
        {
            ctx.try_log(|logger| {
                slog::info!(
                    logger,
                    "Appending block #{} to bitcoin blocks pool",
                    block.height
                )
            });

            match fork_scratch_pad.process_header(block.get_block_header(), ctx) {
                Ok(Some(_event)) => {
                    let _ = observer_commands_tx.send(ObserverCommand::ProcessBitcoinBlock(block));
                }
                Err(e) => {
                    ctx.try_log(|logger| slog::warn!(logger, "Unable to append block: {:?}", e));
                }
                Ok(None) => {
                    ctx.try_log(|logger| slog::warn!(logger, "Unable to append block"));
                }
            }
        } else {
            break;
        }
    }
    Ok(())
}
