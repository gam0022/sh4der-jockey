use std::{
    convert::TryInto,
    io::{ErrorKind, Read},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration,
};

pub const DEFAULT_OSC_HOST: &str = "0.0.0.0";
pub const DEFAULT_OSC_PORT: u16 = 3032;
const BPM_ADDRESS: &str = "/s2l/out/bpm";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OscProtocol {
    Udp,
    Tcp10,
    Tcp11,
}

impl OscProtocol {
    pub fn label(self) -> &'static str {
        match self {
            Self::Udp => "UDP",
            Self::Tcp10 => "TCP 1.0",
            Self::Tcp11 => "TCP 1.1",
        }
    }
}

pub struct Sound2LightOsc {
    bpm_rx: Receiver<f32>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Sound2LightOsc {
    pub fn new(host: &str, port: u16, protocol: OscProtocol) -> Self {
        let (bpm_tx, bpm_rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let worker = spawn_receiver(host.to_owned(), port, protocol, bpm_tx, stop.clone());
        Self {
            bpm_rx,
            stop,
            worker: Some(worker),
        }
    }

    pub fn reconfigure(&mut self, host: &str, port: u16, protocol: OscProtocol) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        *self = Self::new(host, port, protocol);
    }

    pub fn latest_bpm(&self) -> Option<f32> {
        self.bpm_rx.try_iter().last()
    }
}

impl Drop for Sound2LightOsc {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn spawn_receiver(
    host: String,
    port: u16,
    protocol: OscProtocol,
    bpm_tx: Sender<f32>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let result = match protocol {
            OscProtocol::Udp => receive_udp(&host, port, &bpm_tx, &stop),
            OscProtocol::Tcp10 | OscProtocol::Tcp11 => {
                receive_tcp(&host, port, protocol, &bpm_tx, &stop)
            }
        };
        if let Err(err) = result {
            log::error!(
                "Sound2Light OSC {} receiver on {}:{} stopped: {}",
                protocol.label(),
                host,
                port,
                err
            );
        }
    })
}

fn receive_udp(
    host: &str,
    port: u16,
    bpm_tx: &Sender<f32>,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    let socket = UdpSocket::bind((host, port))?;
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    log::info!("Listening for Sound2Light OSC via UDP on {host}:{port}");

    let mut buffer = [0_u8; 65_535];
    while !stop.load(Ordering::Acquire) {
        match socket.recv(&mut buffer) {
            Ok(size) => send_decoded_bpm(&buffer[..size], bpm_tx),
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn receive_tcp(
    host: &str,
    port: u16,
    protocol: OscProtocol,
    bpm_tx: &Sender<f32>,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    let listener = TcpListener::bind((host, port))?;
    listener.set_nonblocking(true)?;
    log::info!(
        "Listening for Sound2Light OSC via {} on {}:{}",
        protocol.label(),
        host,
        port
    );

    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((mut stream, peer)) => {
                log::info!("Sound2Light OSC connected from {peer}");
                stream.set_read_timeout(Some(Duration::from_millis(100)))?;
                let result = match protocol {
                    OscProtocol::Tcp10 => receive_tcp10_stream(&mut stream, bpm_tx, stop),
                    OscProtocol::Tcp11 => receive_tcp11_stream(&mut stream, bpm_tx, stop),
                    OscProtocol::Udp => unreachable!(),
                };
                if let Err(err) = result {
                    log::warn!("Sound2Light OSC connection closed: {err}");
                }
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn receive_tcp10_stream(
    stream: &mut TcpStream,
    bpm_tx: &Sender<f32>,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    while !stop.load(Ordering::Acquire) {
        let mut length_bytes = [0_u8; 4];
        if !read_exact_retry(stream, &mut length_bytes, stop)? {
            return Ok(());
        }
        let length = u32::from_be_bytes(length_bytes) as usize;
        if length == 0 || length > 65_535 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid OSC packet length: {length}"),
            ));
        }
        let mut packet = vec![0_u8; length];
        if !read_exact_retry(stream, &mut packet, stop)? {
            return Ok(());
        }
        send_decoded_bpm(&packet, bpm_tx);
    }
    Ok(())
}

fn receive_tcp11_stream(
    stream: &mut TcpStream,
    bpm_tx: &Sender<f32>,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    const END: u8 = 0xc0;
    const ESC: u8 = 0xdb;
    const ESC_END: u8 = 0xdc;
    const ESC_ESC: u8 = 0xdd;

    let mut frame = Vec::new();
    let mut escaped = false;
    while !stop.load(Ordering::Acquire) {
        let mut byte = [0_u8; 1];
        match stream.read(&mut byte) {
            Ok(0) => return Ok(()),
            Ok(_) => match byte[0] {
                END => {
                    if !frame.is_empty() {
                        send_decoded_bpm(&frame, bpm_tx);
                        frame.clear();
                    }
                    escaped = false;
                }
                ESC => escaped = true,
                ESC_END if escaped => {
                    frame.push(END);
                    escaped = false;
                }
                ESC_ESC if escaped => {
                    frame.push(ESC);
                    escaped = false;
                }
                value => {
                    frame.push(value);
                    escaped = false;
                }
            },
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn read_exact_retry(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    stop: &AtomicBool,
) -> std::io::Result<bool> {
    let mut offset = 0;
    while offset < buffer.len() && !stop.load(Ordering::Acquire) {
        match stream.read(&mut buffer[offset..]) {
            Ok(0) => return Ok(false),
            Ok(size) => offset += size,
            Err(err) if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(offset == buffer.len())
}

fn send_decoded_bpm(packet: &[u8], bpm_tx: &Sender<f32>) {
    if let Some(bpm) = decode_bpm(packet) {
        let _ = bpm_tx.send(bpm);
    }
}

fn decode_bpm(packet: &[u8]) -> Option<f32> {
    let (address, offset) = read_osc_string(packet, 0)?;
    if address != BPM_ADDRESS {
        return None;
    }

    let (type_tags, data_offset) = read_osc_string(packet, offset)?;
    match type_tags.as_bytes().get(1).copied()? {
        b'f' => read_array::<4>(packet, data_offset).map(f32::from_be_bytes),
        b'i' => read_array::<4>(packet, data_offset)
            .map(i32::from_be_bytes)
            .map(|v| v as f32),
        b'd' => read_array::<8>(packet, data_offset)
            .map(f64::from_be_bytes)
            .map(|v| v as f32),
        _ => None,
    }
}

fn read_osc_string(packet: &[u8], offset: usize) -> Option<(&str, usize)> {
    let tail = packet.get(offset..)?;
    let length = tail.iter().position(|&byte| byte == 0)?;
    let value = std::str::from_utf8(&tail[..length]).ok()?;
    let next = offset.checked_add((length + 1 + 3) & !3)?;
    (next <= packet.len()).then_some((value, next))
}

fn read_array<const N: usize>(packet: &[u8], offset: usize) -> Option<[u8; N]> {
    packet.get(offset..offset.checked_add(N)?)?.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_float_bpm_without_splitting_it() {
        let packet = [
            b'/', b's', b'2', b'l', b'/', b'o', b'u', b't', b'/', b'b', b'p', b'm', 0, 0, 0, 0,
            b',', b'f', 0, 0, 0x42, 0xf1, 0, 0,
        ];
        assert_eq!(decode_bpm(&packet), Some(120.5));
    }
}
