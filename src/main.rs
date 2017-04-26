#[macro_use]
extern crate clap;
extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate net2;

use std::net::SocketAddr;
use net2::UdpBuilder;

use tokio_core::reactor::Core;
use tokio_core::net::{UdpSocket, UdpCodec};

use bytes::{Bytes, IntoBuf, Buf};
use futures::{Future, Stream, Sink};
use futures::sync::mpsc::channel;

use clap::{App, Arg};

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
        .get_matches();

    let listen = value_t!(matches.value_of("listen"), SocketAddr).unwrap_or_else(|e| e.exit());
    let backends = values_t!(matches.values_of("backend"), SocketAddr).unwrap_or_else(|e| e.exit());
    let size = value_t!(matches.value_of("queue_size"), usize).unwrap_or_else(|e| e.exit());

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let socket = UdpBuilder::new_v4().unwrap();
    let socket = socket.bind(&listen).unwrap();

    let sender = UdpSocket::from_socket(socket, &handle).unwrap();
    let server = sender.framed(DgramCodec::new(listen));

    let mut chans = Vec::new();

    for b in backends.into_iter() {
        let (tx, rx) = channel::<Bytes>(size);
        chans.push(tx);

        ::std::thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let socket = UdpBuilder::new_v4().unwrap();
            let bind: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let socket = socket.bind(&bind).unwrap();

            let sender = UdpSocket::from_socket(socket, &handle).unwrap();
            let sender = sender
                .framed(DgramCodec::new(b))
                .sink_map_err(|e| println!("SEND ERROR: {:?}", e));
            let sender = rx.forward(sender).map(|_| ());
            core.run(sender)
        });
    }

    let server = server.for_each(move |buf| {
        chans
            .iter_mut()
            .map(|tx| {
                     tx.start_send(buf.clone()).unwrap();
                     tx.poll_complete().unwrap();
                 })
            .last();
        Ok(())
    });

    core.run(server).unwrap();
}
