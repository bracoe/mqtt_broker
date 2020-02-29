use std::io::{Read, Write};
//use std::convert::TryInto;
use std::fs::create_dir_all;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread;
use std::collections::LinkedList;
use std::time::Duration;
//use std::path::Path;
//use std::fs::{File};
use std::env;
//use std::fs;
//use std::fs::OpenOptions;

const MAX_MTU: usize = 15000; //MTU assumed to be 1500

const TYPE_CONNECT: u8 = 1; 
const TYPE_CONNACK: u8 = 2;
const TYPE_PUBLISH: u8 = 3;
const TYPE_PUBACK: u8 = 4;
const TYPE_PUBREC: u8 = 5;
const TYPE_PUBREL: u8 = 6;
const TYPE_PUBCOMP: u8 = 7;
const TYPE_SUBSCRIBE: u8 = 8;
const TYPE_SUBACK: u8 = 9;
const TYPE_UNSUBSCRIBE: u8 = 10;
const TYPE_UNSUBACK: u8 = 11;
const TYPE_PINGREQ: u8 = 12;
const TYPE_PINGRESP: u8 = 13;
const TYPE_DISCONNECT: u8 = 14;

///A structure for saving the entire MQTT message
#[derive(Clone)]
struct MqttMessage{
	message_type:u8,
	dup:u8,
	qos_level:u8,
	retain:u8,
	remaining_length:u8,
	optional_header:Vec<u8>,
	payload: Vec<u8> 
}

fn main() {
    //println!("Hello, world!");

	let result = create_storage_dir();
	assert!(result.is_ok());

	let listener = TcpListener::bind("127.0.0.1:1883").unwrap();
	
	loop{

		match listener.accept(){
			Ok((stream, addr)) => {
				thread::spawn(move || {
					handle_client(stream, addr);
				});
			},
			Err(e) => {
				eprintln!("Error while receiving datagram: {}", e);
			}
		}
	}
}

fn create_storage_dir() -> std::io::Result<()> {
	
	let dir = get_storage_dir_as_string();
	
	create_dir_all(dir)?;
	
	Ok(())
}

fn get_storage_dir_as_string() -> String{
	let path = env::current_dir();
	assert!(path.is_ok());
	let path = path.unwrap();
	let path = path.into_os_string().into_string();
	assert!(path.is_ok());
	let mut path = path.unwrap();
	
	path.push_str("/Storage");
	
	path
	
}

fn handle_client(stream: TcpStream, socket_address: SocketAddr){
	let mut connected = false;
	
	let message = parse_message_from_stream(stream.try_clone().expect("clone failed..."));
	assert!(message.is_ok());
	let message = message.unwrap();
	
	if message.message_type != TYPE_CONNECT{
		//Send nack
	}
	
	let time_out = get_keep_alive_from_conn_msg(message);
	stream.set_read_timeout(Some(time_out)).expect("set_read_timeout call failed");
	stream.set_write_timeout(Some(time_out)).expect("set_read_timeout call failed");
		
	send_connack(stream.try_clone().expect("clone failed..."));
	connected = true;
	
	while connected {
		let message = parse_message_from_stream(stream.try_clone().expect("clone failed..."));
		
		if message.is_err(){
			connected = false;
			break;
		}
		
		let message = message.unwrap();
		
		//execute message request
		println!("Executing message!");
		let result = execute_request(message, stream.try_clone().expect("clone failed..."), socket_address);
		assert!(result.is_ok());
		
	}
	
	
}

fn get_keep_alive_from_conn_msg(message:MqttMessage) -> std::time::Duration{
	if message.optional_header.len() < 2 {
		println!("optional_header is not big enough!");
	}
	
	let opt_header = message.optional_header;
	
	//Get id
	let keep_alive_upper_nibble = (opt_header[8] as u16) << 8;
	let keep_alive_lower_nibble = opt_header[9] as u16;
	
	let mut time = (keep_alive_upper_nibble | keep_alive_lower_nibble) as f64;
	time *= 1.5; //According to OASIS Standard
	let duration = time.ceil() as u64;
	
	Duration::from_millis(duration)
}

fn parse_message_from_stream(mut stream: TcpStream, ) -> Result<MqttMessage, &'static str>{
	let mut tcp_buffer = [0u8; MAX_MTU]; 
	
	let result = stream.read(&mut tcp_buffer);
	
	if result.is_err(){
		return Err("Probably timed-out!")
	}
	
	assert!(result.is_ok());
	let relevant_bytes = result.unwrap();
	
	println!("Relevant bytes are: {}", relevant_bytes);
	
	parse_message_from_buffer(tcp_buffer, relevant_bytes)
	
}

fn parse_message_from_buffer(buffer: [u8; MAX_MTU], relevant_bytes: usize) -> Result<MqttMessage, &'static str>{
	if relevant_bytes < 2 || relevant_bytes > MAX_MTU { //Could not possible have all manditory bytes
		return Err("Invalid message length");
	}
	
	let opt_header_length = buffer[1];
	
	let opt_header = buffer[1..(opt_header_length as usize)].to_vec();
	
	let payload_start = 2 + opt_header_length as usize;
	
	 
	
	
	let message = MqttMessage {
		message_type: (0xF0u8 & buffer[0]) >> 4 as u8,
		dup: (0x08u8 & buffer[0]) >> 3 as u8,
		qos_level: (0x06u8 & buffer[0]) >> 1 as u8,
		retain:0x01u8 & buffer[0] as u8,
		remaining_length:buffer[1],
		optional_header:opt_header,
		payload: buffer[payload_start..relevant_bytes].to_vec()
	};
	
	//println!("Coap header: {}", coap_header);
	
	Ok(message)
}

fn execute_request(message:MqttMessage, stream: TcpStream, _socket_address: SocketAddr) -> Result<&'static str, &'static str>{
	
	match message.message_type {
		
		TYPE_CONNECT => {
			println!("Got type with code: {}", message.message_type);
			//handle_connect_request(message, stream);
		}
		
		TYPE_PINGREQ => {
			send_ping_response(stream);
		}
		
		
		
		_ => {
			println!("Got type with code: {}", message.message_type);
		}
	}
	
	Ok("OK")
}

fn send_ping_response(mut stream: TcpStream){
	let mut response_msg = Vec::<u8>::new();
	response_msg.push(0xD0); //Set type peng response
	response_msg.push(0x00); //Set remaining length to 2 byte
	
	let result = stream.write(&response_msg);
	assert!(result.is_ok());
}

fn send_connack(mut stream: TcpStream){
	let mut response_msg = Vec::<u8>::new();
	response_msg.push(0x20); //Set type connack
	response_msg.push(0x02); //Set remaining length to 2 byte
	response_msg.push(0x00); //Set to connection accepted
	response_msg.push(0x00); //Set to connection accepted
	
	let result = stream.write(&response_msg);
	assert!(result.is_ok());
}
