// We receive this information throw an stream system

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Audit {
    #[prost(string, tag="1")]
    pub user: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub when: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(string, tag="3")]
    pub origin: ::prost::alloc::string::String,
    #[prost(enumeration="audit::Source", tag="4")]
    pub source: i32,
}
/// Nested message and enum types in `Audit`.
pub mod audit {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Source {
        Http = 0,
        Dashboard = 1,
        Desktop = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Error {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration="super::resources::FieldType", tag="2")]
    pub field_type: i32,
    #[prost(string, tag="3")]
    pub error: ::prost::alloc::string::String,
    #[prost(enumeration="error::ErrorCode", tag="4")]
    pub code: i32,
}
/// Nested message and enum types in `Error`.
pub mod error {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ErrorCode {
        Generic = 0,
        Extract = 1,
        Process = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BrokerMessage {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub slug: ::prost::alloc::string::String,
    #[prost(message, optional, tag="5")]
    pub audit: ::core::option::Option<Audit>,
    #[prost(enumeration="broker_message::MessageType", tag="6")]
    pub r#type: i32,
    #[prost(string, tag="7")]
    pub multiid: ::prost::alloc::string::String,
    #[prost(message, optional, tag="8")]
    pub basic: ::core::option::Option<super::resources::Basic>,
    #[prost(message, optional, tag="9")]
    pub origin: ::core::option::Option<super::resources::Origin>,
    #[prost(message, repeated, tag="10")]
    pub relations: ::prost::alloc::vec::Vec<super::utils::Relation>,
    /// Field Conversations
    #[prost(map="string, message", tag="11")]
    pub conversations: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::Conversation>,
    /// Field Layout
    #[prost(map="string, message", tag="12")]
    pub layouts: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldLayout>,
    /// Field Text
    #[prost(map="string, message", tag="13")]
    pub texts: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldText>,
    /// Field keyword
    #[prost(map="string, message", tag="14")]
    pub keywordsets: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldKeywordset>,
    /// Field Datetime
    #[prost(map="string, message", tag="15")]
    pub datetimes: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldDatetime>,
    /// Field Links
    #[prost(map="string, message", tag="16")]
    pub links: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldLink>,
    /// Field File
    #[prost(map="string, message", tag="17")]
    pub files: ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldFile>,
    /// Link extracted extra info
    #[prost(message, repeated, tag="18")]
    pub link_extracted_data: ::prost::alloc::vec::Vec<super::resources::LinkExtractedData>,
    /// File extracted extra info
    #[prost(message, repeated, tag="19")]
    pub file_extracted_data: ::prost::alloc::vec::Vec<super::resources::FileExtractedData>,
    /// Field Extracted/Computed information
    #[prost(message, repeated, tag="20")]
    pub extracted_text: ::prost::alloc::vec::Vec<super::resources::ExtractedTextWrapper>,
    #[prost(message, repeated, tag="21")]
    pub field_metadata: ::prost::alloc::vec::Vec<super::resources::FieldComputedMetadataWrapper>,
    #[prost(message, repeated, tag="22")]
    pub field_vectors: ::prost::alloc::vec::Vec<super::resources::ExtractedVectorsWrapper>,
    /// Resource Large Computed Metadata
    #[prost(message, repeated, tag="23")]
    pub field_large_metadata: ::prost::alloc::vec::Vec<super::resources::LargeComputedMetadataWrapper>,
    #[prost(message, repeated, tag="24")]
    pub delete_fields: ::prost::alloc::vec::Vec<super::resources::FieldId>,
    #[prost(int32, tag="25")]
    pub origin_seq: i32,
    #[prost(float, tag="26")]
    pub slow_processing_time: f32,
    #[prost(float, tag="28")]
    pub pre_processing_time: f32,
    #[prost(message, optional, tag="29")]
    pub done_time: ::core::option::Option<::prost_types::Timestamp>,
    /// Not needed anymore
    #[deprecated]
    #[prost(int64, tag="30")]
    pub txseqid: i64,
    #[prost(message, repeated, tag="31")]
    pub errors: ::prost::alloc::vec::Vec<Error>,
    #[prost(string, tag="32")]
    pub processing_id: ::prost::alloc::string::String,
    #[prost(enumeration="broker_message::MessageSource", tag="33")]
    pub source: i32,
    #[prost(int64, tag="34")]
    pub account_seq: i64,
    #[prost(message, repeated, tag="35")]
    pub user_vectors: ::prost::alloc::vec::Vec<super::resources::UserVectorsWrapper>,
    /// If true, force reindex all paragraphs in a resource
    #[prost(bool, tag="36")]
    pub reindex: bool,
    #[prost(message, optional, tag="37")]
    pub extra: ::core::option::Option<super::resources::Extra>,
    #[prost(message, repeated, tag="38")]
    pub question_answers: ::prost::alloc::vec::Vec<super::resources::FieldQuestionAnswerWrapper>,
}
/// Nested message and enum types in `BrokerMessage`.
pub mod broker_message {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MessageType {
        Autocommit = 0,
        Multi = 1,
        Commit = 2,
        Rollback = 3,
        Delete = 4,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MessageSource {
        Writer = 0,
        Processor = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BrokerMessageBlobReference {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub storage_key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriterStatusResponse {
    #[prost(string, repeated, tag="1")]
    pub knowledgeboxes: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// map of last message processed
    #[prost(map="string, int64", tag="2")]
    pub msgid: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriterStatusRequest {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetLabelsRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub labelset: ::core::option::Option<super::knowledgebox::LabelSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DelLabelsRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelsResponse {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub labels: ::core::option::Option<super::knowledgebox::Labels>,
    #[prost(enumeration="get_labels_response::Status", tag="3")]
    pub status: i32,
}
/// Nested message and enum types in `GetLabelsResponse`.
pub mod get_labels_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelsRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewEntitiesGroupRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub group: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub entities: ::core::option::Option<super::knowledgebox::EntitiesGroup>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewEntitiesGroupResponse {
    #[prost(enumeration="new_entities_group_response::Status", tag="1")]
    pub status: i32,
}
/// Nested message and enum types in `NewEntitiesGroupResponse`.
pub mod new_entities_group_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Error = 1,
        KbNotFound = 2,
        AlreadyExists = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetEntitiesRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub group: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub entities: ::core::option::Option<super::knowledgebox::EntitiesGroup>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateEntitiesGroupRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub group: ::prost::alloc::string::String,
    /// entity_id: Entity
    #[prost(map="string, message", tag="3")]
    pub add: ::std::collections::HashMap<::prost::alloc::string::String, super::knowledgebox::Entity>,
    /// entity_id: Entity
    #[prost(map="string, message", tag="4")]
    pub update: ::std::collections::HashMap<::prost::alloc::string::String, super::knowledgebox::Entity>,
    /// entity_id
    #[prost(string, repeated, tag="5")]
    pub delete: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="6")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub color: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateEntitiesGroupResponse {
    #[prost(enumeration="update_entities_group_response::Status", tag="1")]
    pub status: i32,
}
/// Nested message and enum types in `UpdateEntitiesGroupResponse`.
pub mod update_entities_group_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Error = 1,
        KbNotFound = 2,
        EntitiesGroupNotFound = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEntitiesGroupsRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListEntitiesGroupsResponse {
    #[prost(map="string, message", tag="1")]
    pub groups: ::std::collections::HashMap<::prost::alloc::string::String, super::knowledgebox::EntitiesGroupSummary>,
    #[prost(enumeration="list_entities_groups_response::Status", tag="2")]
    pub status: i32,
}
/// Nested message and enum types in `ListEntitiesGroupsResponse`.
pub mod list_entities_groups_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
        Error = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesResponse {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(map="string, message", tag="2")]
    pub groups: ::std::collections::HashMap<::prost::alloc::string::String, super::knowledgebox::EntitiesGroup>,
    #[prost(enumeration="get_entities_response::Status", tag="3")]
    pub status: i32,
}
/// Nested message and enum types in `GetEntitiesResponse`.
pub mod get_entities_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
        Error = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DelEntitiesRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub group: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MergeEntitiesRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub from: ::core::option::Option<merge_entities_request::EntityId>,
    #[prost(message, optional, tag="3")]
    pub to: ::core::option::Option<merge_entities_request::EntityId>,
}
/// Nested message and enum types in `MergeEntitiesRequest`.
pub mod merge_entities_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntityId {
        #[prost(string, tag="1")]
        pub group: ::prost::alloc::string::String,
        #[prost(string, tag="2")]
        pub entity: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelSetRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub labelset: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelSetResponse {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub labelset: ::core::option::Option<super::knowledgebox::LabelSet>,
    #[prost(enumeration="get_label_set_response::Status", tag="3")]
    pub status: i32,
}
/// Nested message and enum types in `GetLabelSetResponse`.
pub mod get_label_set_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesGroupRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub group: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesGroupResponse {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub group: ::core::option::Option<super::knowledgebox::EntitiesGroup>,
    #[prost(enumeration="get_entities_group_response::Status", tag="3")]
    pub status: i32,
}
/// Nested message and enum types in `GetEntitiesGroupResponse`.
pub mod get_entities_group_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        KbNotFound = 1,
        EntitiesGroupNotFound = 2,
        Error = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetVectorSetsRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetVectorSetsResponse {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub vectorsets: ::core::option::Option<super::knowledgebox::VectorSets>,
    #[prost(enumeration="get_vector_sets_response::Status", tag="3")]
    pub status: i32,
}
/// Nested message and enum types in `GetVectorSetsResponse`.
pub mod get_vector_sets_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
        Error = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DelVectorSetRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub vectorset: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetVectorSetRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag="2")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub vectorset: ::core::option::Option<super::knowledgebox::VectorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpStatusWriter {
    #[prost(enumeration="op_status_writer::Status", tag="1")]
    pub status: i32,
}
/// Nested message and enum types in `OpStatusWriter`.
pub mod op_status_writer {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Error = 1,
        Notfound = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Notification {
    #[prost(int32, tag="1")]
    pub partition: i32,
    #[prost(string, tag="2")]
    pub multi: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(int64, tag="5")]
    pub seqid: i64,
    #[prost(enumeration="notification::Action", tag="6")]
    pub action: i32,
    #[prost(enumeration="notification::WriteType", tag="7")]
    pub write_type: i32,
    #[prost(message, optional, tag="8")]
    pub message: ::core::option::Option<BrokerMessage>,
}
/// Nested message and enum types in `Notification`.
pub mod notification {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Action {
        Commit = 0,
        Abort = 1,
        Indexed = 2,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum WriteType {
        Unset = 0,
        Created = 1,
        Modified = 2,
        Deleted = 3,
    }
}
//// The member information.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Member {
    //// Member ID.　A string of the UUID.
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    //// Cluster listen address. string of IP and port number.
    //// E.g. 127.0.0.1:5000
    #[prost(string, tag="2")]
    pub listen_address: ::prost::alloc::string::String,
    //// If true, it means self.
    #[deprecated]
    #[prost(bool, tag="3")]
    pub is_self: bool,
    //// Io, Ingest, Search, Train.
    #[deprecated]
    #[prost(enumeration="member::Type", tag="4")]
    pub r#type: i32,
    //// Dummy Member
    #[deprecated]
    #[prost(bool, tag="5")]
    pub dummy: bool,
    //// The load score of the member.
    #[deprecated]
    #[prost(float, tag="6")]
    pub load_score: f32,
    //// The number of shards in the node.
    #[prost(uint32, tag="7")]
    pub shard_count: u32,
}
/// Nested message and enum types in `Member`.
pub mod member {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Io = 0,
        Search = 1,
        Ingest = 2,
        Train = 3,
        Unknown = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListMembersRequest {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListMembersResponse {
    #[prost(message, repeated, tag="1")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardReplica {
    #[prost(message, optional, tag="1")]
    pub shard: ::core::option::Option<super::noderesources::ShardCreated>,
    #[prost(string, tag="2")]
    pub node: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardObject {
    #[prost(string, tag="1")]
    pub shard: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="3")]
    pub replicas: ::prost::alloc::vec::Vec<ShardReplica>,
    #[prost(message, optional, tag="4")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shards {
    #[prost(message, repeated, tag="1")]
    pub shards: ::prost::alloc::vec::Vec<ShardObject>,
    #[prost(string, tag="2")]
    pub kbid: ::prost::alloc::string::String,
    /// current shard that resources index to
    #[prost(int32, tag="3")]
    pub actual: i32,
    #[deprecated]
    #[prost(enumeration="super::utils::VectorSimilarity", tag="4")]
    pub similarity: i32,
    #[prost(message, optional, tag="5")]
    pub model: ::core::option::Option<super::knowledgebox::SemanticModelMetadata>,
    #[prost(enumeration="super::utils::ReleaseChannel", tag="6")]
    pub release_channel: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceFieldId {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub rid: ::prost::alloc::string::String,
    #[prost(enumeration="super::resources::FieldType", tag="3")]
    pub field_type: i32,
    #[prost(string, tag="4")]
    pub field: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexResource {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub rid: ::prost::alloc::string::String,
    #[prost(bool, tag="3")]
    pub reindex_vectors: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexStatus {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceFieldExistsResponse {
    #[prost(bool, tag="1")]
    pub found: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceIdRequest {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub slug: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceIdResponse {
    #[prost(string, tag="1")]
    pub uuid: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExportRequest {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetVectorsRequest {
    #[prost(message, optional, tag="1")]
    pub vectors: ::core::option::Option<super::utils::VectorObject>,
    #[prost(string, tag="2")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub rid: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub field: ::core::option::Option<super::resources::FieldId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetVectorsResponse {
    #[prost(bool, tag="1")]
    pub found: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileRequest {
    #[prost(string, tag="1")]
    pub bucket: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryData {
    #[prost(bytes="vec", tag="1")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryMetadata {
    #[prost(string, tag="2")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub key: ::prost::alloc::string::String,
    #[prost(int32, tag="4")]
    pub size: i32,
    #[prost(string, tag="5")]
    pub filename: ::prost::alloc::string::String,
    #[prost(string, tag="6")]
    pub content_type: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UploadBinaryData {
    #[prost(int32, tag="1")]
    pub count: i32,
    #[prost(oneof="upload_binary_data::Data", tags="2, 3")]
    pub data: ::core::option::Option<upload_binary_data::Data>,
}
/// Nested message and enum types in `UploadBinaryData`.
pub mod upload_binary_data {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(message, tag="2")]
        Metadata(super::BinaryMetadata),
        #[prost(bytes, tag="3")]
        Payload(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileUploaded {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SynonymsRequest {
    #[prost(string, tag="1")]
    pub kbid: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetSynonymsRequest {
    #[prost(message, optional, tag="1")]
    pub kbid: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub synonyms: ::core::option::Option<super::knowledgebox::Synonyms>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetSynonymsResponse {
    #[prost(message, optional, tag="1")]
    pub status: ::core::option::Option<OpStatusWriter>,
    #[prost(message, optional, tag="2")]
    pub synonyms: ::core::option::Option<super::knowledgebox::Synonyms>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetKbConfigurationRequest {
    #[prost(message, optional, tag="1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag="2")]
    pub config: ::core::option::Option<super::knowledgebox::KbConfiguration>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetConfigurationResponse {
    #[prost(message, optional, tag="1")]
    pub status: ::core::option::Option<OpStatusWriter>,
    #[prost(message, optional, tag="2")]
    pub config: ::core::option::Option<super::knowledgebox::KbConfiguration>,
}
