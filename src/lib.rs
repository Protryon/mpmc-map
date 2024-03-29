use std::{borrow::Borrow, sync::Arc};

use arc_swap::{ArcSwap, Guard};
use im::HashMap;
use std::hash::Hash;
use tokio::sync::{mpsc, oneshot};

pub type AtomicOp<K, V> = Box<dyn FnOnce(&HashMap<K, V>) -> Option<HashMap<K, V>> + Send>;

enum MpmcMapMutationOp<
    K: Send + Sync + Hash + Clone + Eq + 'static,
    V: Send + Clone + Sync + 'static,
> {
    Reset(Arc<HashMap<K, V>>),
    Atomic(AtomicOp<K, V>),
    Insert(K, V),
    Remove(K),
}

enum MpmcMapMutationResponse<V: Send + Clone + Sync + 'static> {
    None,
    Bool(bool),
    Value(V),
}

struct MpmcMapMutation<
    K: Send + Sync + Hash + Clone + Eq + 'static,
    V: Send + Clone + Sync + 'static,
> {
    op: MpmcMapMutationOp<K, V>,
    response: oneshot::Sender<MpmcMapMutationResponse<V>>,
}

#[derive(Debug)]
pub struct MpmcMap<K: Send + Sync + Hash + Clone + Eq + 'static, V: Send + Clone + Sync + 'static> {
    inner: Arc<ArcSwap<HashMap<K, V>>>,
    sender: mpsc::Sender<MpmcMapMutation<K, V>>,
}

impl<K: Send + Sync + Hash + Clone + Eq + 'static, V: Send + Clone + Sync + 'static> Default
    for MpmcMap<K, V>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Send + Sync + Hash + Clone + Eq + 'static, V: Send + Clone + Sync + 'static> Clone
    for MpmcMap<K, V>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<K: Send + Sync + Hash + Clone + Eq + 'static, V: Send + Clone + Sync + 'static> MpmcMap<K, V> {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(512);

        let new_self = MpmcMap {
            inner: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
            sender,
        };
        tokio::spawn(Self::updater(new_self.inner.clone(), receiver));
        new_self
    }

    async fn updater(
        map: Arc<ArcSwap<HashMap<K, V>>>,
        mut receiver: mpsc::Receiver<MpmcMapMutation<K, V>>,
    ) {
        while let Some(mutation) = receiver.recv().await {
            match mutation.op {
                MpmcMapMutationOp::Insert(key, value) => {
                    let map_load = map.load();
                    if let Some((old_value, prior)) = map_load.extract(&key) {
                        let new_map = prior.update(key, value);
                        map.store(Arc::new(new_map));
                        mutation
                            .response
                            .send(MpmcMapMutationResponse::Value(old_value))
                            .ok();
                    } else {
                        let new_map = map_load.update(key, value);
                        map.store(Arc::new(new_map));
                        mutation.response.send(MpmcMapMutationResponse::None).ok();
                    }
                }
                MpmcMapMutationOp::Remove(key) => {
                    if let Some((old_value, new_map)) = map.load().extract(&key) {
                        map.store(Arc::new(new_map));
                        mutation
                            .response
                            .send(MpmcMapMutationResponse::Value(old_value))
                            .ok();
                    } else {
                        mutation.response.send(MpmcMapMutationResponse::None).ok();
                    }
                }
                MpmcMapMutationOp::Reset(value) => {
                    map.store(value);
                    mutation.response.send(MpmcMapMutationResponse::None).ok();
                }
                MpmcMapMutationOp::Atomic(op) => {
                    let map_load = map.load();
                    let new_map = op(&**map_load);
                    let mutated = new_map.is_some();
                    if let Some(new_map) = new_map {
                        map.store(Arc::new(new_map));
                    }
                    mutation.response.send(MpmcMapMutationResponse::Bool(mutated)).ok();
                }
            }
        }
    }

    pub async fn insert(&self, key: K, value: V) -> Option<V> {
        let (response, receiver) = oneshot::channel::<MpmcMapMutationResponse<V>>();
        self.sender
            .send(MpmcMapMutation {
                op: MpmcMapMutationOp::Insert(key, value),
                response,
            })
            .await
            .ok()
            .expect("failed to send insert mutation");
        match receiver
            .await
            .expect("failed to receive mpmc map mutation response")
        {
            MpmcMapMutationResponse::None => None,
            MpmcMapMutationResponse::Bool(_) => None,
            MpmcMapMutationResponse::Value(v) => Some(v),
        }
    }

    pub async fn remove(&self, key: K) -> Option<V> {
        let (response, receiver) = oneshot::channel::<MpmcMapMutationResponse<V>>();
        self.sender
            .send(MpmcMapMutation {
                op: MpmcMapMutationOp::Remove(key),
                response,
            })
            .await
            .ok()
            .expect("failed to send insert mutation");
        match receiver
            .await
            .expect("failed to receive mpmc map mutation response")
        {
            MpmcMapMutationResponse::None => None,
            MpmcMapMutationResponse::Bool(_) => None,
            MpmcMapMutationResponse::Value(v) => Some(v),
        }
    }

    pub fn get<BK: ?Sized>(&self, key: &BK) -> Option<V>
    where
        BK: Hash + Eq,
        K: Borrow<BK>,
    {
        self.inner.load().get(key).cloned()
    }

    pub fn contains_key<BK: ?Sized>(&self, key: &BK) -> bool
    where
        BK: Hash + Eq,
        K: Borrow<BK>,
    {
        self.inner.load().contains_key(key)
    }

    pub fn inner_full(&self) -> Arc<HashMap<K, V>> {
        self.inner.load_full()
    }

    pub fn inner(&self) -> Guard<Arc<HashMap<K, V>>> {
        self.inner.load()
    }

    // pending updates will be applied to the new value
    // this function should generally be used sparingly, as it can be hard to ensure correct semantics
    // look at `MpmcMap::reset` instead
    #[doc(hidden)]
    pub fn reset_now(&self, value: Arc<HashMap<K, V>>) {
        self.inner.store(value);
    }

    // this function will apply pending updates before reseting the internal map. Those updates will be lost, but will not mutate the new version of the hashmap, `value`.
    pub async fn reset(&self, value: Arc<HashMap<K, V>>) {
        let (response, receiver) = oneshot::channel::<MpmcMapMutationResponse<V>>();
        self.sender
            .send(MpmcMapMutation {
                op: MpmcMapMutationOp::Reset(value),
                response,
            })
            .await
            .ok()
            .expect("failed to send insert mutation");
        receiver
            .await
            .expect("failed to receive mpmc map mutation response");
    }

    // performs an atomic mutation, returns true if a mutation took place (`op` returned `Some`)
    pub async fn atomic(&self, op: AtomicOp<K, V>) -> bool {
        let (response, receiver) = oneshot::channel::<MpmcMapMutationResponse<V>>();
        self.sender
            .send(MpmcMapMutation {
                op: MpmcMapMutationOp::Atomic(op),
                response,
            })
            .await
            .ok()
            .expect("failed to send insert mutation");
        match receiver
            .await
            .expect("failed to receive mpmc map mutation response")
        {
            MpmcMapMutationResponse::None => false,
            MpmcMapMutationResponse::Bool(b) => b,
            MpmcMapMutationResponse::Value(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        self.inner().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner().is_empty()
    }
}
