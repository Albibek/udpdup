#[macro_use]
extern crate clap;
extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate net2;

use std::net::SocketAddr;
use net2::UdpBuilder;
use std::thread;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use tokio_core::reactor::{Core, Interval};
use tokio_core::net::{UdpSocket, UdpCodec};

use bytes::{Bytes, IntoBuf, Buf};
use futures::{Future, Stream, Sink};
use futures::sync::mpsc::channel;

use clap::{App, Arg};

pub static CHUNK_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;

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
             .help("ip and port to listen to")
             .takes_value(true))
        .arg(Arg::with_name("queue_size")
             .short("q")
             .long("queue")
             .takes_value(true)
             .default_value("1048576")
            )
        .arg(Arg::with_name("backend")
             .short("b")
             .long("backend")
             .value_name("IP:PORT")
             .help("IP and port of a backend to forward data to. Can be specified multiple times.")
             .multiple(true)
             .takes_value(true)
            )
         .arg(Arg::with_name("nthreads")
             .short("n")
             .long("nthreads")
             .takes_value(true)
             .default_value("0")
            )
        .get_matches();

    let listen = value_t!(matches.value_of("listen"), SocketAddr).unwrap_or_else(|e| e.exit());
    let backends = values_t!(matches.values_of("backend"), SocketAddr).unwrap_or_else(|e| e.exit());
    let size = value_t!(matches.value_of("queue_size"), usize).unwrap_or_else(|e| e.exit());
    let nthreads = value_t!(matches.value_of("nthreads"), usize).unwrap_or_else(|e| e.exit());

    // Init chunk counter
    // In the main thread start a reporting timer
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(::std::time::Duration::from_secs(1), &handle).unwrap();
    let timer = timer.for_each(|()|{
                println!("chunks: {:?}", CHUNK_COUNTER.load(Ordering::Relaxed));
                CHUNK_COUNTER.store(0, Ordering::Relaxed);
                //println!("total drops: {:?}", METRIC_DROPS.load(Ordering::Relaxed));
                Ok(())
    });

    let socket = UdpBuilder::new_v4().unwrap();
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


    for _ in 1..8 {
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
                         let sender = sender.framed(DgramCodec::new(backends.pop().unwrap()));
                         sender
                     })
                .collect::<Vec<_>>();

            // create server now
            let socket = UdpSocket::from_socket(socket, &handle).unwrap();
            let server = socket.framed(DgramCodec::new(listen));

            let server = server.for_each(move |buf| {
                CHUNK_COUNTER.fetch_add(1, Ordering::Relaxed);
                senders
                    .iter_mut()
                    .map(|sender| {
                             sender.start_send(buf.clone()).unwrap();
                             sender.poll_complete().unwrap();
                         })
                    .last();
                Ok(())
            });

            core.run(server).unwrap();
        });
    }
    core.run(timer).unwrap();
}
