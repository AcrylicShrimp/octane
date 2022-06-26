use dashmap::{mapref::entry::Entry, DashMap};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FileState {
    Allocated,
    Uploading,
}

#[derive(Default, Debug)]
pub struct FileAllocator {
    files: DashMap<Uuid, FileState>,
}

impl FileAllocator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate(&self) -> Uuid {
        let uuid = Uuid::new_v4();
        self.files.insert(uuid, FileState::Allocated);
        uuid
    }

    pub fn free(&self, uuid: &Uuid) {
        self.files.remove(uuid);
    }

    pub fn mark_as_uploading(&self, uuid: Uuid) -> bool {
        match self.files.entry(uuid.into()) {
            Entry::Occupied(mut entry) => {
                if *entry.get() != FileState::Allocated {
                    return false;
                }

                entry.insert(FileState::Uploading);
                true
            }
            Entry::Vacant(_) => false,
        }
    }
}
