extern crate clap;
#[macro_use]
extern crate structopt_derive;
extern crate structopt;
extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate net2;
extern crate num_cpus;
extern crate resolve;

use std::net::SocketAddr;
use std::time::{self, Duration, SystemTime};

use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;
use resolve::resolver;

use std::thread;
use std::sync::atomic::{ATOMIC_USIZE_INIT, AtomicUsize, Ordering};

use tokio_core::reactor::{Core, Interval, Timeout};
use tokio_core::net::{UdpSocket, UdpCodec, TcpStream};
use tokio_io::AsyncWrite;

use bytes::{Bytes, IntoBuf, Buf, BytesMut, BufMut};
use futures::{Stream, Sink, AsyncSink, Future};


use structopt::StructOpt;

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

fn try_resolve(s: &str) -> SocketAddr {
    s.parse()
        .unwrap_or_else(|_| {
            // for name that have failed to be parsed we try to resolve it via DNS
            let mut split = s.split(':');
            let host = split.next().unwrap(); // Split always has first element
            let port = split.next().expect("port not found");
            let port = port.parse().expect("bad port value");

            let first_ip = resolver::resolve_host(host)
                .expect("failed resolving backend name")
                .next()
                .expect("at least one IP address required");
            SocketAddr::new(first_ip, port)
        })
}

#[derive(StructOpt, Debug)]
#[structopt(about = "UDP multiplexor: copies packets from listened address to specified backends dropping packets on any error")]
struct Options {
    #[structopt(short = "l", long = "listen", help = "Address and port to listen to")]
    listen: SocketAddr,

    #[structopt(short = "b", long = "backend", help = "IP and port of a backend to forward data to. Can be specified multiple times.", value_name="IP:PORT")]
    backends: Vec<String>,

    #[structopt(short = "n", long = "nthreads", help = "Number of worker threads, use 0 to use all CPU cores", default_value = "0")]
    nthreads: usize,

    #[structopt(short = "s", long = "size", help = "Internal queue buffer size in packets", default_value = "1048576")]
    bufsize: usize,

    #[structopt(short = "t", long = "interval", help = "Stats printing/sending interval, in milliseconds", default_value = "1000")]
    interval: Option<u64>, // u64 has a special meaning in structopt

    #[structopt(short = "P", long = "stats-prefix", help = "Metric name prefix", default_value="")]
    stats_prefix: String,

    #[structopt(short = "S", long = "stats", help = "Graphite plaintext format compatible address:port to send metrics to")]
    stats: Option<String>,
}

fn main() {
    let mut opts = Options::from_args();

    if opts.nthreads == 0 {
        opts.nthreads = num_cpus::get();
    }

    // Init chunk counter
    // In the main thread start a reporting timer
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let timer = Interval::new(Duration::from_millis(opts.interval.unwrap()), &handle).unwrap();
    let interval = opts.interval.unwrap() as f64 / 1000f64;

    let stats = opts.stats.map(|ref stats| try_resolve(stats));

    let prefix = opts.stats_prefix;

    let timer = timer.for_each(|()|{
        let chunks = CHUNK_COUNTER.swap(0, Ordering::Relaxed) as f64 / interval;
        let drops = DROP_COUNTER.swap(0, Ordering::Relaxed) as f64 / interval;
        let errors = ERR_COUNTER.swap(0, Ordering::Relaxed) as f64 / interval;

        println!("chunks/drops/errors: {:.2}/{:.2}/{:.2}", chunks, drops, errors);

        match stats {
            Some(addr) => {
                let tcp_timeout = Timeout::new(Duration::from_millis(((interval / 2f64) * 1000f64).floor() as u64), &handle)
                    .unwrap()
                    .map_err(|_| ());
                let prefix = prefix.clone();
                let sender = TcpStream::connect(&addr, &handle).and_then(move |mut conn| {
                     let ts = SystemTime::now()
                        .duration_since(time::UNIX_EPOCH).unwrap();
                    let ts = ts.as_secs().to_string();
                    // prefix length can be various sizes, we need capacity for it,
                    // we also need space for other values that are around:
                    // 2 spaces, 7 chars for metric suffix
                    // around 26 chars for f64 value, but cannot be sure it takes more
                    // around 10 chars for timestamp
                    // so minimal value is ~45, we'll take 128 just in case float is very large
                    // for 3 metrics this must be tripled
                    let mut buf = BytesMut::with_capacity((prefix.len() + 128)*3);

                    buf.put(&prefix);
                    buf.put(".udpdup.chunks ");
                    buf.put(chunks.to_string());
                    buf.put(" ");
                    buf.put(&ts);
                    buf.put("\n");

                    buf.put(&prefix);
                    buf.put(".udpdup.drops ");
                    buf.put(drops.to_string());
                    buf.put(" ");
                    buf.put(&ts);
                    buf.put("\n");

                    buf.put(&prefix);
                    buf.put(".udpdup.errors ");
                    buf.put(errors.to_string());
                    buf.put(" ");
                    buf.put(&ts);

                    conn.write_buf(&mut buf.into_buf()).map(|_|())
                }).map_err(|e| println!("Error sending stats: {:?}", e)).select(tcp_timeout).then(|_|Ok(()));
               handle.spawn(sender);
            }
            None => (),
        };
        Ok(())
    });

    // Now the min server work
    let socket = UdpBuilder::new_v4().unwrap();
    socket.reuse_address(true).unwrap();
    socket.reuse_port(true).unwrap();
    let socket = socket.bind(&opts.listen).unwrap();

    let backends = opts.backends
        .iter()
        .map(|b| try_resolve(b))
        .collect::<Vec<SocketAddr>>();

    // Create sockets for each backend address
    let senders = backends
        .iter()
        .map(|_| {
                 let socket = UdpBuilder::new_v4().unwrap();
                 let bind: SocketAddr = "0.0.0.0:0".parse().unwrap();
                 socket.bind(&bind).unwrap()
             })
        .collect::<Vec<_>>();

    let bufsize = opts.bufsize;
    let listen = opts.listen;
    for _ in 0..opts.nthreads {
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
