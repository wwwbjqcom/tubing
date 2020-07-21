/*
@author: xiao cai niao
@datetime: 2019/9/23
*/

use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::process;
use tracing::field::debug;


pub fn get_sha1_pass(password: &String, auth_plugin_name: &String, auth_plugin_data: &Vec<u8>) -> Vec<u8> {
    if auth_plugin_name == &String::from("mysql_native_password"){
        match scramble_native(&auth_plugin_data[..20], password.as_bytes()){
            Some(value) => {return value.to_vec();},
            None => process::exit(1)

        };
    }else if auth_plugin_name == &String::from("caching_sha2_password") {
        match scramble_sha256( &auth_plugin_data[..20], password.as_bytes()){
            Some(value) => {return value.to_vec()},
            None => process::exit(1)

        };
    }
    debug(format!("Unsupported password verification method：{}", auth_plugin_name));
    process::exit(1);
}


fn xor<T, U>(mut left: T, right: U) -> T
    where
        T: AsMut<[u8]>,
        U: AsRef<[u8]>,
{
    left.as_mut()
        .iter_mut()
        .zip(right.as_ref().iter())
        .map(|(l, r)| *l ^= r)
        .last();
    left
}

fn to_u8_32(bytes: impl AsRef<[u8]>) -> [u8; 32] {
    let mut out = [0; 32];
    (&mut out[..]).copy_from_slice(bytes.as_ref());
    out
}

///mysql_native_password
///SHA1(password) XOR SHA1(nonce, SHA1(SHA1(password)))

pub fn scramble_native(nonce: &[u8], password: &[u8]) -> Option<[u8; 20]> {
    fn sha1_1(bytes: impl AsRef<[u8]>) -> [u8; 20] {
        let mut hasher = Sha1::default();
        hasher.update(bytes.as_ref());
        hasher.digest().bytes()
    }

    fn sha1_2(bytes1: impl AsRef<[u8]>, bytes2: impl AsRef<[u8]>) -> [u8; 20] {
        let mut hasher = Sha1::default();
        hasher.update(bytes1.as_ref());
        hasher.update(bytes2.as_ref());
        hasher.digest().bytes()
    }

    if password.len() == 0 {
        return None;
    }

    Some(xor(
        sha1_1(password),
        sha1_2(nonce, sha1_1(sha1_1(password))),
    ))
}

/// cached_sha2_password .
///
/// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), nonce))
pub fn scramble_sha256(nonce: &[u8], password: &[u8]) -> Option<[u8; 32]> {
    fn sha256_1(bytes: impl AsRef<[u8]>) -> [u8; 32] {
        let mut hasher = Sha256::default();
        hasher.input(bytes.as_ref());
        to_u8_32(hasher.result())
    }

    fn sha256_2(bytes1: impl AsRef<[u8]>, bytes2: impl AsRef<[u8]>) -> [u8; 32] {
        let mut hasher = Sha256::default();
        hasher.input(bytes1.as_ref());
        hasher.input(bytes2.as_ref());
        to_u8_32(hasher.result())
    }

    if password.len() == 0 {
        return None;
    }

    Some(xor(
        sha256_1(password),
        sha256_2(sha256_1(sha256_1(password)), nonce),
    ))
}

