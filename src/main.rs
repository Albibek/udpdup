#[macro_use]
extern crate clap;
extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate net2;

use std::net::SocketAddr;
use std::time::Duration;

use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use std::thread;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, UdpCodec};

use bytes::{Bytes, IntoBuf, Buf};
use futures::{Stream, Sink, AsyncSink};

use clap::{App, Arg};

pub static CHUNK_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
pub static DROP_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;
pub static ERR_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;


struct DgramCodec {
    addr: SocketAddr,
}

impl DgramCodec {
    pub fn new(addr: SocketAddr) -> Self {
        DgramCodec { addr: addr }
    }
}

impl UdpCodec for DgramCodec {
    type In = Bytes;
    type Out = Bytes;

    fn decode(&mut self,
              _src: &SocketAddr,
              buf: &[u8])
              -> ::std::result::Result<Self::In, ::std::io::Error> {
        Ok(buf.into())
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> SocketAddr {
        use std::io::Read;
        msg.into_buf().reader().read_to_end(buf).unwrap();
        self.addr
    }
}

fn main() {
    let matches = App::new("MEGA UDP DUPLICATOR")
        .version("0.0.0.0.0.0.1alpha")
        .author("Sergey Noskov <snoskov@avito.ru>")
        .about("UDP multiplexor: copies packets from listened address to specified backends")
        .arg(Arg::with_name("listen")
             .short("l")
             .long("listen")
             .value_name("IP:PORT")
             .takes_value(true)
             .help("ip and port to listen to")
            )
        .arg(Arg::with_name("backend")
             .short("b")
             .long("backend")
             .value_name("IP:PORT")
             .multiple(true)
             .takes_value(true)
             .help("IP and port of a backend to forward data to. Can be specified multiple times.")
            )
        .arg(Arg::with_name("bufsize")
             .help("Internal queue buffer size")
             .short("s")
             .long("size")
             .takes_value(true)
             .default_value("1048576")
            )
        .arg(Arg::with_name("nthreads")
             .short("n")
             .long("nthreads")
             .takes_value(true)
             .default_value("0")
            )
        .arg(Arg::with_name("interval")
             .short("t")
             .long("interval")
             .takes_value(true)
             .default_value("1000")
             .help("Stats printing interval (in milliseconds)")
            )
        .get_matches();

    let listen = value_t!(matches.value_of("listen"), SocketAddr).unwrap_or_else(|e| e.exit());
    let backends = values_t!(matches.values_of("backend"), SocketAddr).unwrap_or_else(|e| e.exit());
    let nthreads = value_t!(matches.value_of("nthreads"), usize).unwrap_or_else(|e| e.exit());
    let bufsize = value_t!(matches.value_of("bufsize"), usize).unwrap_or_else(|e| e.exit());
    let interval = value_t!(matches.value_of("interval"), u64).unwrap_or_else(|e| e.exit());

    // Init chunk counter
    // In the main thread start a reporting timer
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(Duration::from_millis(interval), &handle).unwrap();
    let timer = timer.for_each(|()|{
        let interval = interval as f64 / 1000f64;
        let chunks = CHUNK_COUNTER.load(Ordering::Relaxed) as f64 / interval;
        let drops = DROP_COUNTER.load(Ordering::Relaxed) as f64 / interval;
        let errors = ERR_COUNTER.load(Ordering::Relaxed) as f64 / interval;

    println!("chunks/drops/errors: {:.2}/{:.2}/{:.2}", chunks, drops, errors);
    CHUNK_COUNTER.store(0, Ordering::Relaxed);
    DROP_COUNTER.store(0, Ordering::Relaxed);
    ERR_COUNTER.store(0, Ordering::Relaxed);
    Ok(())
    });

    let socket = UdpBuilder::new_v4().unwrap();
    socket.reuse_address(true).unwrap();
    socket.reuse_port(true).unwrap();
    let socket = socket.bind(&listen).unwrap();

    // Create sockets from backend addresses
    let senders = backends
        .iter()
        .map(|_| {
                 let socket = UdpBuilder::new_v4().unwrap();
                 let bind: SocketAddr = "0.0.0.0:0".parse().unwrap();
                 socket.bind(&bind).unwrap()
             })
        .collect::<Vec<_>>();


    for _ in 0..nthreads {
        // create clone of each sender socket for thread
        let senders = senders
            .iter()
            .map(|socket| socket.try_clone().unwrap())
            .collect::<Vec<_>>();

        let socket = socket.try_clone().unwrap();
        let mut backends = backends.clone();
        thread::spawn(move || {
            // each thread runs it's own core
            let mut core = Core::new().unwrap();
            let handle = core.handle();

            // make senders into framed UDP
            let mut senders = senders
                .into_iter()
                .map(|socket| {
                    let sender = UdpSocket::from_socket(socket, &handle).unwrap();
                    // socket order becomes reversed but we don't care about this
                    // because the order is same everywhere
                    // and anyways, we push same data to every socket
                    let sender = sender
                        .framed(DgramCodec::new(backends.pop().unwrap()))
                        .buffer(bufsize);
                    sender
                })
                .collect::<Vec<_>>();

            // create server now
            let socket = UdpSocket::from_socket(socket, &handle).unwrap();
            let server = socket.framed(DgramCodec::new(listen)).buffer(bufsize);

            let server = server.for_each(move |buf| {
                CHUNK_COUNTER.fetch_add(1, Ordering::Relaxed);
                senders
                    .iter_mut()
                    .map(|sender| {
                        let res = sender.start_send(buf.clone());
                        match res {
                            Err(_) => {
                                ERR_COUNTER.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(AsyncSink::NotReady(_)) => {
                                DROP_COUNTER.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(AsyncSink::Ready) => {
                                let res = sender.poll_complete();
                                if res.is_err() {
                                    ERR_COUNTER.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    })
                    .last();
                Ok(())
            });

            core.run(server).unwrap();
        });
    }
    core.run(timer).unwrap();
}
