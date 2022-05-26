use std::path::Path;

use heed::flags::Flags;
use heed::types::{ByteSlice, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoPrefix, RoTxn, RwTxn};
use nucliadb_byte_rpr::*;

use crate::edge::*;
use crate::identifier::*;
use crate::node::*;

const LMDB_ENV: &str = "ENV_lmdb";
const KEYS_DB: &str = "KEYS_LMDB";
const RESOURCES_DB: &str = "RESOURCES_LMDB";
const ENTITIES_DB: &str = "ENTITIES_LMDB";
const LABELS_DB: &str = "LABELS_LMDB";
const COLABORATORS_DB: &str = "COLABS_LMDB";
const EDGE_DB: &str = "EDGE_LMDB";
const STATE_DB: &str = "STATE_LMDB";
const STAMP: &str = "stamp.nuclia";
const MAP_SIZE: usize = 1048576 * 100000;
const MAX_DBS: u32 = 3000;

mod storage_state {
    pub const FRESH_RESOURCE: &str = "fresh_resource";
    pub const FRESH_ENTITY: &str = "fresh_entity";
    pub const FRESH_LABEL: &str = "fresh_label";
    pub const FRESH_COLABORATOR: &str = "fresh_colaborator";
}

pub struct RoToken<'a>(RoTxn<'a>);
impl<'a> std::ops::Deref for RoToken<'a> {
    type Target = RoTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a> std::ops::DerefMut for RoToken<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
pub struct RwToken<'a>(RwTxn<'a, 'a>);
impl<'a> std::ops::Deref for RwToken<'a> {
    type Target = RwTxn<'a, 'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a> std::ops::DerefMut for RwToken<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct EdgeIter<'a> {
    iter: RoPrefix<'a, Str, Unit>,
}
impl<'a> Iterator for EdgeIter<'a> {
    type Item = Edge;
    fn next(&mut self) -> Option<Edge> {
        self.iter
            .next()
            .transpose()
            .unwrap()
            .map(|(k, _)| Edge::from(k))
    }
}

#[derive(Clone, Debug, Copy)]
enum NodeId {
    Resource(ResourceID),
    Entity(EntityID),
    Label(LabelID),
    Colaborator(ColaboratorID),
}

impl ByteRpr for NodeId {
    fn as_byte_rpr(&self) -> Vec<u8> {
        let mut buff = vec![];
        match self {
            NodeId::Resource(id) => {
                buff.append(&mut 0u64.as_byte_rpr());
                buff.append(&mut id.as_byte_rpr());
            }
            NodeId::Entity(id) => {
                buff.append(&mut 1u64.as_byte_rpr());
                buff.append(&mut id.as_byte_rpr());
            }
            NodeId::Label(id) => {
                buff.append(&mut 2u64.as_byte_rpr());
                buff.append(&mut id.as_byte_rpr());
            }
            NodeId::Colaborator(id) => {
                buff.append(&mut 3u64.as_byte_rpr());
                buff.append(&mut id.as_byte_rpr());
            }
        }
        buff
    }

    fn from_byte_rpr(bytes: &[u8]) -> Self {
        let discriminant_start = 0;
        let discriminant_end = discriminant_start + u64::segment_len();
        let id_start = discriminant_end;
        let id_end = discriminant_end + ResourceID::segment_len();
        let id = &bytes[id_start..id_end];
        match u64::from_byte_rpr(&bytes[discriminant_start..discriminant_end]) {
            0 => NodeId::Resource(ResourceID::from_byte_rpr(id)),
            1 => NodeId::Entity(EntityID::from_byte_rpr(id)),
            2 => NodeId::Label(LabelID::from_byte_rpr(id)),
            3 => NodeId::Colaborator(ColaboratorID::from_byte_rpr(id)),
            _ => panic!("Invalid node id"),
        }
    }
}

impl FixedByteLen for NodeId {
    fn segment_len() -> usize {
        ResourceID::segment_len() + u64::segment_len()
    }
}

pub struct StorageSystem {
    env: Env,
    // key -> NodeId
    keys: Database<Str, ByteSlice>,
    // ResourceID -> ResourceData
    resources: Database<ByteSlice, ByteSlice>,
    // EntityID -> EntityData
    entities: Database<ByteSlice, ByteSlice>,
    // LabelID -> LabelData
    labels: Database<ByteSlice, ByteSlice>,
    // ColaboratorID -> ColabData
    colaborators: Database<ByteSlice, ByteSlice>,
    // Edges of the graph
    edges: Database<Str, Unit>,
    // Name of the field -> current value
    state: Database<Str, ByteSlice>,
}

impl StorageSystem {
    pub fn create(path: &Path) -> StorageSystem {
        let env_path = path.join(LMDB_ENV);
        if !env_path.exists() {
            std::fs::create_dir_all(&env_path).unwrap();
            let mut env_builder = EnvOpenOptions::new();
            env_builder.max_dbs(MAX_DBS);
            env_builder.map_size(MAP_SIZE);
            unsafe {
                env_builder.flag(Flags::MdbNoLock);
            }
            let env = env_builder.open(&env_path).unwrap();
            let keys = env.create_database(Some(KEYS_DB)).unwrap();
            let resources = env.create_database(Some(RESOURCES_DB)).unwrap();
            let entities = env.create_database(Some(ENTITIES_DB)).unwrap();
            let labels = env.create_database(Some(LABELS_DB)).unwrap();
            let colaborators = env.create_database(Some(COLABORATORS_DB)).unwrap();
            let edges = env.create_database(Some(EDGE_DB)).unwrap();
            let state = env.create_database(Some(STATE_DB)).unwrap();
            StorageSystem {
                env,
                keys,
                resources,
                entities,
                labels,
                colaborators,
                edges,
                state,
            }
        } else {
            StorageSystem::open(path)
        }
    }

    pub fn open(path: &Path) -> StorageSystem {
        let sleep_time = std::time::Duration::from_millis(20);
        let env_path = path.join(LMDB_ENV);
        let stamp_path = path.join(STAMP);
        while !stamp_path.exists() {
            std::thread::sleep(sleep_time);
        }
        let mut env_builder = EnvOpenOptions::new();
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        let env = env_builder.open(&env_path).unwrap();
        let keys = env.open_database(Some(KEYS_DB)).unwrap().unwrap();
        let resources = env.open_database(Some(RESOURCES_DB)).unwrap().unwrap();
        let entities = env.open_database(Some(ENTITIES_DB)).unwrap().unwrap();
        let labels = env.open_database(Some(LABELS_DB)).unwrap().unwrap();
        let colaborators = env.open_database(Some(COLABORATORS_DB)).unwrap().unwrap();
        let edges = env.open_database(Some(EDGE_DB)).unwrap().unwrap();
        let state = env.open_database(Some(STATE_DB)).unwrap().unwrap();
        StorageSystem {
            env,
            keys,
            resources,
            entities,
            labels,
            colaborators,
            edges,
            state,
        }
    }

    pub fn rw_txn(&self) -> RwToken<'_> {
        RwToken(self.env.write_txn().unwrap())
    }
    pub fn ro_txn(&self) -> RoToken<'_> {
        RoToken(self.env.read_txn().unwrap())
    }
    pub fn add_resource(&self, txn: &mut RwTxn, data: ResourceData) -> bool {
        let mut had_effect = false;
        if self.keys.get(txn, &data.name).unwrap().is_none() {
            let resource_id = self.get_fresh_resource_id(txn);
            let node_id = NodeId::Resource(resource_id);
            self.keys
                .put(txn, &data.name, &node_id.as_byte_rpr())
                .unwrap();
            self.resources
                .put(txn, &resource_id.as_byte_rpr(), &data.as_byte_rpr())
                .unwrap();
            had_effect = true;
        }
        had_effect
    }
    pub fn get_resource_id(&self, txn: &RoTxn, name: &str) -> Option<ResourceID> {
        match self.keys.get(txn, name).unwrap().map(NodeId::from_byte_rpr) {
            Some(NodeId::Resource(id)) => Some(id),
            _ => None,
        }
    }
    pub fn get_resource(&self, txn: &RoTxn, id: ResourceID) -> Option<ResourceData> {
        self.resources
            .get(txn, &id.as_byte_rpr())
            .unwrap()
            .map(ResourceData::from_byte_rpr)
    }
    pub fn add_entity(&self, txn: &mut RwTxn, data: EntityData) -> bool {
        let mut had_effect = false;
        if self.keys.get(txn, &data.name).unwrap().is_none() {
            let entity_id = self.get_fresh_entity_id(txn);
            let node_id = NodeId::Entity(entity_id);
            self.keys
                .put(txn, &data.name, &node_id.as_byte_rpr())
                .unwrap();
            self.entities
                .put(txn, &entity_id.as_byte_rpr(), &data.as_byte_rpr())
                .unwrap();
            had_effect = true;
        }
        had_effect
    }
    pub fn get_entity_id(&self, txn: &RoTxn, name: &str) -> Option<EntityID> {
        match self.keys.get(txn, name).unwrap().map(NodeId::from_byte_rpr) {
            Some(NodeId::Entity(id)) => Some(id),
            _ => None,
        }
    }
    pub fn get_entity(&self, txn: &RoTxn, id: EntityID) -> Option<EntityData> {
        self.entities
            .get(txn, &id.as_byte_rpr())
            .unwrap()
            .map(EntityData::from_byte_rpr)
    }
    pub fn add_label(&self, txn: &mut RwTxn, data: LabelData) -> bool {
        let mut had_effect = false;
        if self.keys.get(txn, &data.name).unwrap().is_none() {
            let label_id = self.get_fresh_label_id(txn);
            let node_id = NodeId::Label(label_id);
            self.keys
                .put(txn, &data.name, &node_id.as_byte_rpr())
                .unwrap();
            self.labels
                .put(txn, &label_id.as_byte_rpr(), &data.as_byte_rpr())
                .unwrap();
            had_effect = true;
        }
        had_effect
    }
    pub fn get_label_id(&self, txn: &RoTxn, name: &str) -> Option<LabelID> {
        match self.keys.get(txn, name).unwrap().map(NodeId::from_byte_rpr) {
            Some(NodeId::Label(id)) => Some(id),
            _ => None,
        }
    }
    pub fn get_label(&self, txn: &RoTxn, id: LabelID) -> Option<LabelData> {
        self.labels
            .get(txn, &id.as_byte_rpr())
            .unwrap()
            .map(LabelData::from_byte_rpr)
    }
    pub fn add_colaborator(&self, txn: &mut RwTxn, data: ColabData) -> bool {
        let mut had_effect = false;
        if self.keys.get(txn, &data.name).unwrap().is_none() {
            let colab_id = self.get_fresh_colaborator_id(txn);
            let node_id = NodeId::Colaborator(colab_id);
            self.keys
                .put(txn, &data.name, &node_id.as_byte_rpr())
                .unwrap();
            self.colaborators
                .put(txn, &colab_id.as_byte_rpr(), &data.as_byte_rpr())
                .unwrap();
            had_effect = true;
        }
        had_effect
    }
    pub fn get_colaborator_id(&self, txn: &RoTxn, name: &str) -> Option<ColaboratorID> {
        match self.keys.get(txn, name).unwrap().map(NodeId::from_byte_rpr) {
            Some(NodeId::Colaborator(id)) => Some(id),
            _ => None,
        }
    }
    pub fn get_colaborator(&self, txn: &RoTxn, id: ColaboratorID) -> Option<ColabData> {
        self.colaborators
            .get(txn, &id.as_byte_rpr())
            .unwrap()
            .map(ColabData::from_byte_rpr)
    }
    pub fn add_edge(&self, txn: &mut RwTxn, edge: Edge) -> bool {
        let edge_fmt = edge.to_string();
        let mut had_effect = false;
        if self.edges.get(txn, &edge_fmt).unwrap().is_none() {
            self.edges.put(txn, &edge.to_string(), &()).unwrap();
            had_effect = true;
        }
        had_effect
    }
    pub fn process_query<'a>(&self, txn: &'a RoTxn, query: Query) -> EdgeIter<'a> {
        let query_formated = query.to_string();
        let iter = self.edges.prefix_iter(txn, &query_formated).unwrap();
        EdgeIter { iter }
    }
    fn get_fresh_resource_id(&self, txn: &mut RwTxn) -> ResourceID {
        let mut fresh = self
            .state
            .get(txn, storage_state::FRESH_RESOURCE)
            .unwrap()
            .map(ResourceID::from_byte_rpr)
            .unwrap_or_else(ResourceID::new);
        let current = fresh.next();
        self.state
            .put(txn, storage_state::FRESH_RESOURCE, &fresh.as_byte_rpr())
            .unwrap();
        current
    }
    fn get_fresh_entity_id(&self, txn: &mut RwTxn) -> EntityID {
        let mut fresh = self
            .state
            .get(txn, storage_state::FRESH_ENTITY)
            .unwrap()
            .map(EntityID::from_byte_rpr)
            .unwrap_or_else(EntityID::new);
        let current = fresh.next();
        self.state
            .put(txn, storage_state::FRESH_ENTITY, &fresh.as_byte_rpr())
            .unwrap();
        current
    }
    fn get_fresh_label_id(&self, txn: &mut RwTxn) -> LabelID {
        let mut fresh = self
            .state
            .get(txn, storage_state::FRESH_LABEL)
            .unwrap()
            .map(LabelID::from_byte_rpr)
            .unwrap_or_else(LabelID::new);
        let current = fresh.next();
        self.state
            .put(txn, storage_state::FRESH_LABEL, &fresh.as_byte_rpr())
            .unwrap();
        current
    }
    fn get_fresh_colaborator_id(&self, txn: &mut RwTxn) -> ColaboratorID {
        let mut fresh = self
            .state
            .get(txn, storage_state::FRESH_COLABORATOR)
            .unwrap()
            .map(ColaboratorID::from_byte_rpr)
            .unwrap_or_else(ColaboratorID::new);
        let current = fresh.next();
        self.state
            .put(txn, storage_state::FRESH_COLABORATOR, &fresh.as_byte_rpr())
            .unwrap();
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn initialize_storage_system() -> StorageSystem {
        let dir = tempfile::tempdir().unwrap();
        StorageSystem::create(dir.path())
    }
    #[test]
    fn add_resource() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let resource = ResourceData {
            name: "Name".to_string(),
        };
        assert!(system.add_resource(&mut txn, resource));
        assert!(system.get_resource_id(&txn, "Name").is_some());
        assert!(system.get_resource_id(&txn, "Nonexistent").is_none());
        assert!(system
            .get_resource(&txn, system.get_resource_id(&txn, "Name").unwrap())
            .is_some());
        assert_eq!(
            system
                .get_resource(&txn, system.get_resource_id(&txn, "Name").unwrap())
                .unwrap()
                .name,
            "Name"
        );
    }
    #[test]
    fn same_resource_same_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let resource0 = ResourceData {
            name: "Name".to_string(),
        };
        let resource1 = ResourceData {
            name: "Name".to_string(),
        };
        assert!(system.add_resource(&mut txn, resource0));
        assert!(!system.add_resource(&mut txn, resource1));
    }
    #[test]
    fn different_resource_different_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let resource0 = ResourceData {
            name: "Name0".to_string(),
        };
        let resource1 = ResourceData {
            name: "Name1".to_string(),
        };
        assert!(system.add_resource(&mut txn, resource0));
        assert!(system.add_resource(&mut txn, resource1));
        assert!(system.get_resource_id(&txn, "Name0").is_some());
        assert!(system.get_resource_id(&txn, "Name1").is_some());
        assert_ne!(
            system.get_resource_id(&txn, "Name0").unwrap(),
            system.get_resource_id(&txn, "Name1").unwrap()
        );
        assert_ne!(
            system
                .get_resource(&txn, system.get_resource_id(&txn, "Name0").unwrap())
                .unwrap()
                .name,
            system
                .get_resource(&txn, system.get_resource_id(&txn, "Name1").unwrap())
                .unwrap()
                .name,
        )
    }
    #[test]
    fn add_entity() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let entity = EntityData {
            name: "Name".to_string(),
        };
        assert!(system.add_entity(&mut txn, entity));
        assert!(system.get_entity_id(&txn, "Name").is_some());
        assert!(system.get_entity_id(&txn, "Nonexistent").is_none());
        assert!(system
            .get_entity(&txn, system.get_entity_id(&txn, "Name").unwrap())
            .is_some());
        assert_eq!(
            system
                .get_entity(&txn, system.get_entity_id(&txn, "Name").unwrap())
                .unwrap()
                .name,
            "Name"
        );
    }
    #[test]
    fn same_entity_same_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let entity0 = EntityData {
            name: "Name".to_string(),
        };
        let entity1 = EntityData {
            name: "Name".to_string(),
        };
        assert!(system.add_entity(&mut txn, entity0));
        assert!(!system.add_entity(&mut txn, entity1));
    }
    #[test]
    fn different_entity_different_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let entity0 = EntityData {
            name: "Name0".to_string(),
        };
        let entity1 = EntityData {
            name: "Name1".to_string(),
        };
        assert!(system.add_entity(&mut txn, entity0));
        assert!(system.add_entity(&mut txn, entity1));
        assert!(system.get_entity_id(&txn, "Name0").is_some());
        assert!(system.get_entity_id(&txn, "Name1").is_some());
        assert_ne!(
            system.get_entity_id(&txn, "Name0").unwrap(),
            system.get_entity_id(&txn, "Name1").unwrap()
        );
        assert_ne!(
            system
                .get_entity(&txn, system.get_entity_id(&txn, "Name0").unwrap())
                .unwrap()
                .name,
            system
                .get_entity(&txn, system.get_entity_id(&txn, "Name1").unwrap())
                .unwrap()
                .name,
        )
    }
    #[test]
    fn add_label() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let label = LabelData {
            name: "Name".to_string(),
        };
        assert!(system.add_label(&mut txn, label));
        assert!(system.get_label_id(&txn, "Name").is_some());
        assert!(system.get_label_id(&txn, "Nonexistent").is_none());
        assert!(system
            .get_label(&txn, system.get_label_id(&txn, "Name").unwrap())
            .is_some());
        assert_eq!(
            system
                .get_label(&txn, system.get_label_id(&txn, "Name").unwrap())
                .unwrap()
                .name,
            "Name"
        );
    }
    #[test]
    fn same_label_same_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let label0 = LabelData {
            name: "Name".to_string(),
        };
        let label1 = LabelData {
            name: "Name".to_string(),
        };
        assert!(system.add_label(&mut txn, label0));
        assert!(!system.add_label(&mut txn, label1));
    }
    #[test]
    fn different_label_different_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let label0 = LabelData {
            name: "Name0".to_string(),
        };
        let label1 = LabelData {
            name: "Name1".to_string(),
        };
        assert!(system.add_label(&mut txn, label0));
        assert!(system.add_label(&mut txn, label1));
        assert!(system.get_label_id(&txn, "Name0").is_some());
        assert!(system.get_label_id(&txn, "Name1").is_some());
        assert_ne!(
            system.get_label_id(&txn, "Name0").unwrap(),
            system.get_label_id(&txn, "Name1").unwrap()
        );
        assert_ne!(
            system
                .get_label(&txn, system.get_label_id(&txn, "Name0").unwrap())
                .unwrap()
                .name,
            system
                .get_label(&txn, system.get_label_id(&txn, "Name1").unwrap())
                .unwrap()
                .name,
        )
    }
    #[test]
    fn add_colaborator() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let colaborator = ColabData {
            name: "Name".to_string(),
        };
        assert!(system.add_colaborator(&mut txn, colaborator));
        assert!(system.get_colaborator_id(&txn, "Name").is_some());
        assert!(system.get_colaborator_id(&txn, "Nonexistent").is_none());
        assert!(system
            .get_colaborator(&txn, system.get_colaborator_id(&txn, "Name").unwrap())
            .is_some());
        assert_eq!(
            system
                .get_colaborator(&txn, system.get_colaborator_id(&txn, "Name").unwrap())
                .unwrap()
                .name,
            "Name"
        );
    }
    #[test]
    fn same_colaborator_same_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let colaborator0 = ColabData {
            name: "Name".to_string(),
        };
        let colaborator1 = ColabData {
            name: "Name".to_string(),
        };
        assert!(system.add_colaborator(&mut txn, colaborator0));
        assert!(!system.add_colaborator(&mut txn, colaborator1));
    }
    #[test]
    fn different_colaborator_different_id() {
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let colaborator0 = ColabData {
            name: "Name0".to_string(),
        };
        let colaborator1 = ColabData {
            name: "Name1".to_string(),
        };
        assert!(system.add_colaborator(&mut txn, colaborator0));
        assert!(system.add_colaborator(&mut txn, colaborator1));
        assert!(system.get_colaborator_id(&txn, "Name0").is_some());
        assert!(system.get_colaborator_id(&txn, "Name1").is_some());
        assert_ne!(
            system.get_colaborator_id(&txn, "Name0").unwrap(),
            system.get_colaborator_id(&txn, "Name1").unwrap()
        );
        assert_ne!(
            system
                .get_colaborator(&txn, system.get_colaborator_id(&txn, "Name0").unwrap())
                .unwrap()
                .name,
            system
                .get_colaborator(&txn, system.get_colaborator_id(&txn, "Name1").unwrap())
                .unwrap()
                .name,
        )
    }

    #[test]
    fn graph_querying() {
        use std::collections::HashSet;
        let system = initialize_storage_system();
        let mut txn = system.rw_txn();
        let r0 = ResourceData {
            name: "R0".to_string(),
        };
        let r1 = ResourceData {
            name: "R1".to_string(),
        };
        let l0 = LabelData {
            name: "L0".to_string(),
        };
        let l1 = LabelData {
            name: "L1".to_string(),
        };
        system.add_resource(&mut txn, r0);
        system.add_resource(&mut txn, r1);
        system.add_label(&mut txn, l0);
        system.add_label(&mut txn, l1);
        let r0 = system.get_resource_id(&txn, "R0").unwrap();
        let r1 = system.get_resource_id(&txn, "R1").unwrap();
        let l0 = system.get_label_id(&txn, "L0").unwrap();
        let l1 = system.get_label_id(&txn, "L1").unwrap();

        // Edges
        let edges_r0 = HashSet::from([Edge::Child(r0, r1), Edge::About(r0, l0)]);
        let edges_r1 = HashSet::from([Edge::About(r1, l0), Edge::About(r1, l1)]);
        assert!(edges_r0.iter().all(|edge| system.add_edge(&mut txn, *edge)));
        assert!(edges_r1.iter().all(|edge| system.add_edge(&mut txn, *edge)));
        assert!(edges_r0
            .iter()
            .all(|edge| !system.add_edge(&mut txn, *edge)));
        assert!(edges_r1
            .iter()
            .all(|edge| !system.add_edge(&mut txn, *edge)));
        {
            let result: HashSet<_> = system.process_query(&txn, Query::AllR(r0)).collect();
            assert_eq!(result, edges_r0);
            let result: HashSet<_> = system.process_query(&txn, Query::AllR(r1)).collect();
            assert_eq!(result, edges_r1);
        }
        {
            let result: HashSet<_> = system
                .process_query(&txn, Query::ChildQ(r0, None))
                .collect();
            assert_eq!(result, HashSet::from([Edge::Child(r0, r1)]));
            let result: HashSet<_> = system
                .process_query(&txn, Query::ChildQ(r1, None))
                .collect();
            assert_eq!(result, HashSet::from([]));
        }
        {
            let result: HashSet<_> = system
                .process_query(&txn, Query::ChildQ(r0, None))
                .collect();
            assert_eq!(result, HashSet::from([Edge::Child(r0, r1)]));
            let result: HashSet<_> = system
                .process_query(&txn, Query::ChildQ(r1, None))
                .collect();
            assert_eq!(result, HashSet::from([]));
        }
        {
            let result: HashSet<_> = system
                .process_query(&txn, Query::AboutQ(r0, None))
                .collect();
            assert_eq!(result, HashSet::from([Edge::About(r0, l0)]));
            let result: HashSet<_> = system
                .process_query(&txn, Query::AboutQ(r1, None))
                .collect();
            assert_eq!(
                result,
                HashSet::from([Edge::About(r1, l0), Edge::About(r1, l1)])
            )
        }
        {
            let results_r0: HashSet<_> = system
                .process_query(&txn, Query::AboutQ(r0, None))
                .collect();
            let results_r1: HashSet<_> = system
                .process_query(&txn, Query::AboutQ(r0, None))
                .collect();
            let intersection = HashSet::from([Edge::About(r0, l0)]);
            assert_eq!(results_r0.intersection(&results_r1).count(), 1);
            assert!(results_r0
                .intersection(&results_r1)
                .all(|e| intersection.contains(e)));
        }
    }
}
