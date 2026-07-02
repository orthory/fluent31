//! Loopback latency/throughput probe: sequential GETs, then pipelined.
use fluent_wire::WireClient;
use std::time::Instant;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let addr = std::env::args().nth(1).unwrap_or("127.0.0.1:8427".into());
    let c = WireClient::connect(&addr).await.unwrap();
    c.put(b"bench", b"value-payload-64-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").await.unwrap();

    const SEQ: usize = 5000;
    let t0 = Instant::now();
    for _ in 0..SEQ {
        c.get(b"bench").await.unwrap();
    }
    let el = t0.elapsed();
    println!("sequential: {SEQ} GETs in {el:?} = {:.1}µs/op, {:.0} ops/s",
        el.as_micros() as f64 / SEQ as f64, SEQ as f64 / el.as_secs_f64());

    const PIPE: usize = 50_000;
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(PIPE);
    for _ in 0..PIPE {
        let c = c.clone();
        handles.push(tokio::spawn(async move { c.get(b"bench").await.unwrap() }));
    }
    for h in handles { h.await.unwrap(); }
    let el = t0.elapsed();
    println!("pipelined:  {PIPE} GETs in {el:?} = {:.0} ops/s",
        PIPE as f64 / el.as_secs_f64());
}
