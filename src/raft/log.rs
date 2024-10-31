use crate::raft::Error;
use std::{fmt::Debug, vec::Vec};

pub struct Log<T> {
    entries: Vec<T>,
    terms: Vec<u64>,
}

impl<T> Debug for Log<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T> Log<T> {
    pub fn new() -> Self {
        Self {
            entries: vec![],
            terms: vec![],
        }
    }

    pub fn append(&mut self, term: u64, entry: T) -> u64 {
        assert!(self.entries.len() == self.terms.len());
        self.entries.push(entry);
        self.terms.push(term);
        self.entries.len() as _
    }

    pub fn entry(&self, index: u64) -> Option<&T> {
        let index = index.checked_sub(1)?;
        self.entries.get(index as usize)
    }

    pub fn term(&self, index: u64) -> Option<u64> {
        let index = index.checked_sub(1)?;
        self.terms.get(index as usize).cloned()
    }

    pub fn term_and_entry(&self, index: u64) -> Option<(u64, &T)> {
        if self.entry(index).is_none() {
            return None;
        }
        Some((self.term(index).unwrap(), self.entry(index).unwrap()))
    }

    pub fn force_write(&mut self, index: u64, term: u64, entry: T) {
        self.entries[index as usize] = entry;
        self.terms[index as usize] = term;
    }

    pub fn len(&self) -> u64 {
        self.entries.len() as _
    }
}
