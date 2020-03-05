extern crate chashmap;

use chashmap::CHashMap;
use std::io::{Read, Write};
//use std::convert::TryInto;
use std::fs::create_dir_all;
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread;
use std::collections::{LinkedList, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::time;
//use std::path::Path;
//use std::fs::{File};
use std::env;
use std::str;
//use std::fs;
//use std::fs::OpenOptions;



const MAX_MTU: usize = 150_000; //MTU assumed to be 1500

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

	let sub_map: Arc<RwLock<HashMap<String, Vec<SocketAddr>>>> = Arc::new(RwLock::new(HashMap::new()));

	let result = create_storage_dir();
	assert!(result.is_ok());

	let listener = TcpListener::bind("127.0.0.1:1883").unwrap();
	
	loop{
		
		let sub_map_clone = sub_map.clone();
	
		match listener.accept(){
			Ok((stream, addr)) => {
				thread::spawn(move || {
					handle_client(stream, addr, &sub_map_clone);
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

fn handle_client(mut stream: TcpStream, socket_address: SocketAddr, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>){
	let mut connected = false;
	
	loop {
		
		let message_list = parse_message_from_stream(stream.try_clone().expect("clone failed..."));
		
		if message_list.is_err(){
			println!("Breaking because error");
			break;
		}
		
		assert!(message_list.is_ok());
		let mut message_list = message_list.unwrap();
		
		while !message_list.is_empty(){
		
			let message = message_list.pop_front().unwrap();
			
			if message.message_type == TYPE_CONNECT{
				/*let time_out = get_keep_alive_from_conn_msg(message.clone());
				stream.set_read_timeout(Some(time_out)).expect("set_read_timeout call failed");
				stream.set_write_timeout(Some(time_out)).expect("set_read_timeout call failed");
				*/
				connected = true;
			}
			else if message.message_type == TYPE_DISCONNECT {
				println!("Breaking because disconnected");
				break;
			}
			
			//execute message request
			println!("Executing message!");
			let result = execute_request(message, socket_address, stream.try_clone().expect("clone failed..."), sub_map);
			assert!(result.is_ok());
		}
		
		if !connected {
			println!("Breaking because not connected");
			break;
		}
	}
	
	println!("Exiting thread");
	
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


fn parse_message_from_stream(mut stream: TcpStream, ) -> Result<LinkedList<MqttMessage>, &'static str>{
	let mut tcp_buffer = [0u8; MAX_MTU]; 
	
	let result = stream.read(&mut tcp_buffer);
	
	let relevant_bytes = match result {
        Ok(relevant_bytes) => relevant_bytes,
        Err(error) => {
            panic!("Problem reading from socket: {:?}", error)
        },
    };
	
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
		
		let message_type = (0xF0u8 & buffer[pos]) >> 4 as u8;
		let dup = (0x08u8 & buffer[pos]) >> 3 as u8;
		let qos_level = (0x06u8 & buffer[pos]) >> 1 as u8;
		let	retain = 0x01u8 & buffer[pos] as u8;
		let total_remaining_length = buffer[pos+1] as usize;
		let optional_header;
		let payload;
		
		pos += 2; //Header is over, next is opt header
		
		if (message_type == TYPE_SUBSCRIBE) || (message_type == TYPE_UNSUBSCRIBE) {
			optional_header = buffer[pos..pos+2].to_vec();
			pos += 2;  
			payload = buffer[pos..pos+(total_remaining_length-2)].to_vec();
			pos += total_remaining_length-2;
		}
		else {
			optional_header = buffer[pos..pos+total_remaining_length].to_vec();
			pos += total_remaining_length; //Header is over, next is next message.
			payload = Vec::<u8>::new();
		}
		
		let message = MqttMessage {
			message_type,
			dup,
			qos_level,
			retain,
			remaining_length: total_remaining_length as u8,
			optional_header,
			payload,
		};
		
		println!("Found message!");
		println!("Type: {}", message.message_type);
		println!("DUP: {}", message.dup);
		println!("QOS: {}", message.qos_level);
		println!("Retain: {}", message.retain);
		println!("Remaining Length: {}", message.remaining_length);
		
		println!("Payload length: {}", total_remaining_length);
		println!("Relevant bytes in stream are {} current pos is {}", relevant_bytes, pos);
		//println!("Optional Header: {}", message.optional_header);
		//println!("Payload: {}", message.payload);
		
		message_list.push_back(message); 
	}
	
	
	//println!("Coap header: {}", coap_header);
	
	Ok(message_list)
}

fn execute_request(message:MqttMessage, socket_address: SocketAddr, stream: TcpStream, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>) -> Result<&'static str, &'static str>{
	
	match message.message_type {
		TYPE_CONNECT => {
			handle_connect_request(stream);
		}
		
		TYPE_PINGREQ => {
			send_ping_response(stream);
		}
		
		TYPE_SUBSCRIBE => {
			handle_sub_request(message, stream, socket_address, sub_map);
		}
		
		TYPE_UNSUBSCRIBE => {
			handle_unsub_request(message, stream, socket_address, sub_map);
		}
		
		TYPE_PUBLISH => {
			handle_publish_request(message, stream, sub_map);
		}
		
		
		
		_ => {
			println!("No implementation for code: {}", message.message_type);
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
	println!("We have send a ping response!");
}

fn send_connack(mut stream: TcpStream){
	let mut response_msg = Vec::<u8>::new();
	response_msg.push(0x20); //Set type connack
	response_msg.push(0x02); //Set remaining length to 2 byte
	response_msg.push(0x00); //Set to connection accepted
	response_msg.push(0x00); //Set to connection accepted
	
	let result = stream.write(&response_msg);
	assert!(result.is_ok());
	println!("We have send a connack!");
	
	let stream = stream.flush();
	assert!(stream.is_ok());
}

fn send_suback(mut stream: TcpStream, packet_id: Vec<u8>){
	let mut response_msg = Vec::<u8>::new();
	response_msg.push(0x90); //Set type suback
	response_msg.push(0x03); //Set remaining length to 2 byte
	response_msg.push(packet_id[0]); //Set type connack
	response_msg.push(packet_id[1]); //Set remaining length to 2 byte
	response_msg.push(0x00); //Success maximum QOS 0
	
	let result = stream.write(&response_msg);
	assert!(result.is_ok());
	
	let stream = stream.flush();
	assert!(stream.is_ok());
	
	println!("We have send a suback!");
}

fn send_unsuback(mut stream: TcpStream, packet_id: Vec<u8>){
	let mut response_msg = Vec::<u8>::new();
	response_msg.push(0xA2); //Set type unsuback
	response_msg.push(0x02); //Set remaining length to 2 byte
	response_msg.push(packet_id[0]); //Set type connack
	response_msg.push(packet_id[1]); //Set remaining length to 2 byte
	
	let result = stream.write(&response_msg);
	assert!(result.is_ok());
	
	let stream = stream.flush();
	assert!(stream.is_ok());
	
	println!("We have send a unsuback!");
}

fn handle_connect_request(stream: TcpStream){
	send_connack(stream);
}

fn handle_sub_request(message:MqttMessage, stream: TcpStream, socket_address: SocketAddr, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>){
	
	let packet_id = message.optional_header.clone();
	let topic_list = get_sub_topic_list(message);
	assert!(topic_list.is_ok());
	let mut topic_list = topic_list.unwrap();
	
	while !topic_list.is_empty() {
		let topic = topic_list.pop_front();
		let topic = topic.unwrap();
		
		println!("We are handling topic: {}", String::from_utf8(topic.clone()).unwrap());
		
		add_sub_to_map(topic, socket_address, sub_map);
	}
	
	send_suback(stream, packet_id);
}

fn handle_unsub_request(message:MqttMessage, stream: TcpStream, socket_address: SocketAddr, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>){
	
	let packet_id = message.optional_header.clone();
	let topic_list = get_sub_topic_list(message);
	assert!(topic_list.is_ok());
	let mut topic_list = topic_list.unwrap();
	
	while !topic_list.is_empty() {
		let topic = topic_list.pop_front();
		let topic = topic.unwrap();
		
		println!("We are handling topic: {}", String::from_utf8(topic.clone()).unwrap());
		
		remove_sub_from_map(topic, socket_address, sub_map);
	}
	
	send_unsuback(stream, packet_id);
}

fn get_sub_topic_list(message:MqttMessage) -> Result<LinkedList<Vec<u8>>, &'static str>{
	let payload = message.payload;
	
	if payload.is_empty(){
		return Err("Payload is empty");
	}
	
	let last_pos = payload.len()-2; //we start at 0 and last byte is reserved
	let mut pos = 0;
	
	let mut topic_list: LinkedList<Vec<u8>> = LinkedList::new();
	
	println!("We are getting new topics from payload length: {}", last_pos);
	
	while pos < last_pos{
		
		let upper_length_nibble = (payload[pos] as u16) << 8;
		let lower_length_nibble = payload[pos+1] as u16;
		let topic_length = (upper_length_nibble | lower_length_nibble) as usize;
		
		pos += 2;
		
		let topic = payload[pos..pos+topic_length].to_vec();
		
		topic_list.push_back(topic.clone());
		println!("Added Topic to list: {}", str::from_utf8(&topic).unwrap());
		pos += topic_length;
	} 
	
	Ok(topic_list)
}

fn add_sub_to_map(topic:Vec<u8>, socket_address: SocketAddr, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>){
	
	let topic_string = String::from_utf8(topic);
	assert!(topic_string.is_ok());
	let topic_string = topic_string.unwrap();
	
	let unlocked_sub_map = sub_map.write();
	let mut unlocked_sub_map = unlocked_sub_map.unwrap();
	
	match unlocked_sub_map.get_mut(&topic_string) {
        Some(sub_list) => {
			if !sub_list.contains(&socket_address){
				sub_list.push(socket_address);
			}
		}
        None => {
			let mut sub_list: Vec<SocketAddr> = Vec::new();
			sub_list.push(socket_address);
			unlocked_sub_map.insert(topic_string, sub_list);
		}	
    }
}

fn remove_sub_from_map(topic:Vec<u8>, socket_address: SocketAddr, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>){
	
	let topic_string = String::from_utf8(topic);
	assert!(topic_string.is_ok());
	let topic_string = topic_string.unwrap();
	
	let unlocked_sub_map = sub_map.write();
	let mut unlocked_sub_map = unlocked_sub_map.unwrap();
	
	match unlocked_sub_map.get_mut(&topic_string) {
        Some(sub_list) => {
			if sub_list.contains(&socket_address){
				let index = sub_list.iter().position(|x| *x == socket_address).unwrap();
				sub_list.remove(index);
			}
		}
		None => {
			println!("Did not find in subs for topic: {}", topic_string);
		}
    }
}

fn handle_publish_request(message:MqttMessage, _stream: TcpStream, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>){
	
	let opt_header = message.optional_header.clone();
	let topic = get_publish_topic(opt_header);
		
	println!("We are handling topic: {}", String::from_utf8(topic.clone()).unwrap());
	
	let mut subs_list = get_subs_for_topic(topic, sub_map);
	
	while subs_list.is_empty(){
		let sub_socket = subs_list.pop();
		let sub_socket = sub_socket.unwrap();
		
		send_publish_msg(message.clone(), sub_socket);
		println!("Publishing to subscriber");
	}
	
	/*if message.qos_level == 1{
		send_puback(stream);
	}*/
}

fn send_publish_msg(message:MqttMessage, socket_address: SocketAddr){
	let buffer = mqtt_msg_to_u8_vec(message);
	
	let stream = TcpStream::connect(socket_address);
	assert!(stream.is_ok());
	
	let mut stream = stream.unwrap();
	
    let result = stream.write(&buffer);
	assert!(result.is_ok());
	
	let stream = stream.flush();
	assert!(stream.is_ok());
	
	println!("We have sent publish to {}", socket_address);
	
}

fn mqtt_msg_to_u8_vec(message: MqttMessage) -> Vec<u8>{
	
	let first_byte = (message.message_type << 4) | (message.dup << 3) | (message.qos_level << 1) | message.retain;
	
	let mut buffer = Vec::<u8>::new();
	let mut opt_header = message.optional_header.clone();
	let mut payload = message.payload.clone();
	
	buffer.push(first_byte);
	buffer.push(message.remaining_length);
	buffer.append(&mut opt_header);
	buffer.append(&mut payload);
	
	buffer
	
}

fn get_publish_topic(optional_header:Vec<u8>) -> Vec<u8>{
	
	assert!(optional_header.len() > 4);
	
	let upper_length_nibble = (optional_header[0] as u16) << 8;
	let lower_length_nibble = optional_header[1] as u16;
	let topic_length = (upper_length_nibble | lower_length_nibble) as usize;
	
	optional_header[2..2+topic_length].to_vec()
	

}

fn get_subs_for_topic(topic:Vec<u8>, sub_map: &Arc<RwLock<HashMap<String, Vec<SocketAddr>>>>) -> Vec<SocketAddr>{
	
	let topic_string = String::from_utf8(topic);
	assert!(topic_string.is_ok());
	let topic_string = topic_string.unwrap();
	
	let unlocked_sub_map = sub_map.write();
	let mut unlocked_sub_map = unlocked_sub_map.unwrap();
	
	match unlocked_sub_map.get_mut(&topic_string) {
        Some(sub_list) => {
			sub_list.clone()
		}
		None => {
			println!("Did not find in subs for topic: {}", topic_string);
			Vec::new()
		}
    }
}
