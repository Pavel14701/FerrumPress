use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Role {
    Subscriber = 0,
    Author = 10,
    Editor = 20,
    Admin = 30,
}

impl Role {
    pub fn from_repr(val: u8) -> Option<Self> {
        match val {
            0 => Some(Self::Subscriber),
            10 => Some(Self::Author),
            20 => Some(Self::Editor),
            30 => Some(Self::Admin),
            _ => None,
        }
    }
}