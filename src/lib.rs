pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/corestream.rs"));
}

pub mod storage;
