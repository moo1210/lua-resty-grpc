//! A generation-counted slab, keyed by an opaque `u64` handle, for
//! handing out Rust-owned values across the FFI boundary as plain
//! integers instead of pointers. Each map here backs exactly one
//! handle type (`CLIENTS` for `grpc_client_handle`, `CALLS` for
//! `grpc_call_handle`, both in `lib.rs`), and the FFI function that
//! decodes a handle already knows which map it belongs to, so there's
//! no cross-map type tag to check. Reusing a slot after deletion bumps
//! its generation counter, so a handle from before the delete resolves
//! to `HandleError` instead of whatever now occupies that slot.
//!
//! A handle is never `0`: `grpc.lua` uses that value as its "call
//! failed to start" / "no client" sentinel, so the generation counter
//! starts at 1 and skips back over 0 on wraparound.

#[derive(Debug)]
pub struct HandleError;

const INDEX_BITS: u32 = 32;

struct Slot<T> {
  generation: u32,
  value: Option<T>,
}

pub struct HandleMap<T> {
  slots: Vec<Slot<T>>,
  free: Vec<u32>,
}

impl<T> HandleMap<T> {
  pub fn new() -> Self {
    Self {
      slots: Vec::new(),
      free: Vec::new(),
    }
  }

  pub fn insert(&mut self, value: T) -> u64 {
    let (index, generation) = match self.free.pop() {
      Some(index) => {
        let slot = &mut self.slots[index as usize];
        slot.value = Some(value);
        (index, slot.generation)
      }
      None => {
        let index = self.slots.len() as u32;
        self.slots.push(Slot {
          generation: 1,
          value: Some(value),
        });
        (index, 1)
      }
    };
    ((generation as u64) << INDEX_BITS) | index as u64
  }

  pub fn get(&self, handle: u64) -> Result<&T, HandleError> {
    let (index, generation) = unpack(handle);
    match self.slots.get(index as usize) {
      Some(slot) if slot.generation == generation => slot.value.as_ref().ok_or(HandleError),
      _ => Err(HandleError),
    }
  }

  pub fn delete(&mut self, handle: u64) -> Result<(), HandleError> {
    let (index, generation) = unpack(handle);
    let slot = self.slots.get_mut(index as usize).ok_or(HandleError)?;
    if slot.generation != generation || slot.value.take().is_none() {
      return Err(HandleError);
    }
    slot.generation = match slot.generation.wrapping_add(1) {
      0 => 1,
      next => next,
    };
    self.free.push(index);
    Ok(())
  }
}

fn unpack(handle: u64) -> (u32, u32) {
  (handle as u32, (handle >> INDEX_BITS) as u32)
}
