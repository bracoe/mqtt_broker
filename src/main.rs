extern crate chashmap;

use chashmap::CHashMap;
use std::io::{Read, Write};
//use std::convert::TryInto;
use std::fs::create_dir_all;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread;
use std::collections::{LinkedList, hash_map};
use std::time::Duration;
//use std::path::Path;
//use std::fs::{File};
use std::env;
use std::str;
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

	let mut sub_map: CHashMap<&str, LinkedList<SocketAddr>> = CHashMap::new();
	

	let result = create_storage_dir();
	assert!(result.is_ok());

	let listener = TcpListener::bind("127.0.0.1:1883").unwrap();
	
	loop{

		match listener.accept(){
			Ok((stream, addr)) => {
				thread::spawn(move || {
					handle_client(stream, addr, sub_map);
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

fn handle_client(stream: TcpStream, socket_address: SocketAddr, sub_map: CHashMap<&str , LinkedList<SocketAddr>>){
		
	let message_list = parse_message_from_stream(stream.try_clone().expect("clone failed..."));
	assert!(message_list.is_ok());
	let mut message_list = message_list.unwrap();
	
	while !message_list.is_empty(){
		
		let message = message_list.pop_front().unwrap();
		
		//execute message request
		println!("Executing message!");
		let result = execute_request(message, socket_address, stream.try_clone().expect("clone failed..."), sub_map);
		assert!(result.is_ok());
	}	
}

fn parse_message_from_stream(mut stream: TcpStream, ) -> Result<LinkedList<MqttMessage>, &'static str>{
	let mut tcp_buffer = [0u8; MAX_MTU]; 
	
	let result = stream.read(&mut tcp_buffer);
	
	if result.is_err(){
		return Err("Probably timed-out!")
	}
	
	assert!(result.is_ok());
	let relevant_bytes = result.unwrap();
	
	println!("Relevant bytes are: {}", relevant_bytes);
	
	parse_messages_from_buffer(tcp_buffer, relevant_bytes)
	
}

fn parse_messages_from_buffer(buffer: [u8; MAX_MTU], relevant_bytes: usize) -> Result<LinkedList<MqttMessage>, &'static str>{
	if relevant_bytes < 2 || relevant_bytes > MAX_MTU { //Could not possible have all manditory bytes
		return Err("Invalid message length");
	}
	
	
	let mut message_list: LinkedList<MqttMessage> = LinkedList::new();
	let mut pos = 0;
	
	while pos < relevant_bytes {
		
		let opt_header_length = buffer[pos+1] as usize;
		let opt_header = buffer[pos..opt_header_length].to_vec();
		let payload_start = pos + 2 + opt_header_length;
		let payload_length = ((opt_header[0] <<8) | opt_header[1]) as usize;
		
		let message = MqttMessage {
			message_type: (0xF0u8 & buffer[pos]) >> 4 as u8,
			dup: (0x08u8 & buffer[pos]) >> 3 as u8,
			qos_level: (0x06u8 & buffer[pos]) >> 1 as u8,
			retain:0x01u8 & buffer[pos] as u8,
			remaining_length:buffer[pos+1],
			optional_header:opt_header,
			payload: buffer[payload_start..payload_length].to_vec()
		};
		
		message_list.push_back(message);
		
		pos = payload_length + opt_header_length + 2; 
	}
	
	
	//println!("Coap header: {}", coap_header);
	
	Ok(message_list)
}

fn execute_request(message:MqttMessage, socket_address: SocketAddr, stream: TcpStream, sub_map: CHashMap<&str, LinkedList<SocketAddr>>) -> Result<&'static str, &'static str>{
	
	match message.message_type {
		
		TYPE_CONNECT => {
			println!("Got type with code: {}", message.message_type);
			handle_connect_request(stream);
		}
		
		TYPE_PINGREQ => {
			send_ping_response(stream);
		}
		
		TYPE_SUBSCRIBE => {
			handle_sub_request(message, socket_address, sub_map);
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

fn handle_connect_request(stream: TcpStream){
	send_connack(stream);
}

fn handle_sub_request(message:MqttMessage, socket_address: SocketAddr, sub_map: CHashMap<&str, LinkedList<SocketAddr>>){
	let mut topic_list = get_topic_list(message);
	
	while !topic_list.is_empty() {
		let topic = topic_list.pop_front().unwrap();
		
		add_sub_to_map(topic, stream, sub_map);
	}
}

fn get_topic_list(message:MqttMessage) -> LinkedList<Vec<u8>>{
	let payload = message.payload;
	let last_pos = payload.len()-1;
	let mut pos = 0;
	
	let mut topic_list: LinkedList<Vec<u8>> = LinkedList::new();
	
	while pos < last_pos{
		let topic_length = ((payload[pos] <<8) | payload[pos+1]) as usize;
		pos += 2;
		
		let topic = payload[pos..topic_length].to_vec();
		
		topic_list.push_back(topic);
		println!("Added Topic to list: {}", str::from_utf8(&topic).unwrap());
		pos += topic_length;
	} 
	
	topic_list
}

fn add_sub_to_map(topic:Vec<u8>, socket_address: SocketAddr, sub_map: CHashMap<&str, LinkedList<SocketAddr>>){
	
	let topic_string = str::from_utf8(&topic).unwrap();
	
	match sub_map.get(topic_string) {
        Some(sub_list) => {
			if !sub_list.contains(&socket_address){
				sub_list.push_back(socket_address);
			}
		}
        None => {
			let mut sub_list: LinkedList<SocketAddr> = LinkedList::new();
			sub_list.push_back(socket_address);
			sub_map.insert(topic_string, sub_list);
		}	
    }
}
