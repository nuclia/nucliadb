// We receive this information throw an stream system

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Audit {
    #[prost(string, tag = "1")]
    pub user: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub when: ::core::option::Option<::prost_wkt_types::Timestamp>,
    #[prost(string, tag = "3")]
    pub origin: ::prost::alloc::string::String,
    #[prost(enumeration = "audit::Source", tag = "4")]
    pub source: i32,
}
/// Nested message and enum types in `Audit`.
pub mod audit {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Source {
        Http = 0,
        Dashboard = 1,
        Desktop = 2,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Error {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration = "super::resources::FieldType", tag = "2")]
    pub field_type: i32,
    #[prost(string, tag = "3")]
    pub error: ::prost::alloc::string::String,
    #[prost(enumeration = "error::ErrorCode", tag = "4")]
    pub code: i32,
}
/// Nested message and enum types in `Error`.
pub mod error {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ErrorCode {
        Generic = 0,
        Extract = 1,
        Process = 2,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BrokerMessage {
    #[prost(string, tag = "1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub slug: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "5")]
    pub audit: ::core::option::Option<Audit>,
    #[prost(enumeration = "broker_message::MessageType", tag = "6")]
    pub r#type: i32,
    #[prost(string, tag = "7")]
    pub multiid: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "8")]
    pub basic: ::core::option::Option<super::resources::Basic>,
    #[prost(message, optional, tag = "9")]
    pub origin: ::core::option::Option<super::resources::Origin>,
    #[prost(message, repeated, tag = "10")]
    pub relations: ::prost::alloc::vec::Vec<super::utils::Relation>,
    /// Field Conversations
    #[prost(map = "string, message", tag = "11")]
    pub conversations:
        ::std::collections::HashMap<::prost::alloc::string::String, super::resources::Conversation>,
    /// Field Layout
    #[prost(map = "string, message", tag = "12")]
    pub layouts:
        ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldLayout>,
    /// Field Text
    #[prost(map = "string, message", tag = "13")]
    pub texts:
        ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldText>,
    /// Field keyword
    #[prost(map = "string, message", tag = "14")]
    pub keywordsets: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        super::resources::FieldKeywordset,
    >,
    /// Field Datetime
    #[prost(map = "string, message", tag = "15")]
    pub datetimes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        super::resources::FieldDatetime,
    >,
    /// Field Links
    #[prost(map = "string, message", tag = "16")]
    pub links:
        ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldLink>,
    /// Field File
    #[prost(map = "string, message", tag = "17")]
    pub files:
        ::std::collections::HashMap<::prost::alloc::string::String, super::resources::FieldFile>,
    /// Link extracted extra info
    #[prost(message, repeated, tag = "18")]
    pub link_extracted_data: ::prost::alloc::vec::Vec<super::resources::LinkExtractedData>,
    /// File extracted extra info
    #[prost(message, repeated, tag = "19")]
    pub file_extracted_data: ::prost::alloc::vec::Vec<super::resources::FileExtractedData>,
    /// Field Extracted/Computed information
    #[prost(message, repeated, tag = "20")]
    pub extracted_text: ::prost::alloc::vec::Vec<super::resources::ExtractedTextWrapper>,
    #[prost(message, repeated, tag = "21")]
    pub field_metadata: ::prost::alloc::vec::Vec<super::resources::FieldComputedMetadataWrapper>,
    #[prost(message, repeated, tag = "22")]
    pub field_vectors: ::prost::alloc::vec::Vec<super::resources::ExtractedVectorsWrapper>,
    /// Resource Large Computed Metadata
    #[prost(message, repeated, tag = "23")]
    pub field_large_metadata:
        ::prost::alloc::vec::Vec<super::resources::LargeComputedMetadataWrapper>,
    #[prost(message, repeated, tag = "24")]
    pub delete_fields: ::prost::alloc::vec::Vec<super::resources::FieldId>,
    #[prost(int32, tag = "25")]
    pub origin_seq: i32,
    #[prost(float, tag = "26")]
    pub slow_processing_time: f32,
    #[prost(float, tag = "28")]
    pub pre_processing_time: f32,
    #[prost(message, optional, tag = "29")]
    pub done_time: ::core::option::Option<::prost_wkt_types::Timestamp>,
    #[prost(int64, tag = "30")]
    pub txseqid: i64,
    #[prost(message, repeated, tag = "31")]
    pub errors: ::prost::alloc::vec::Vec<Error>,
}
/// Nested message and enum types in `BrokerMessage`.
pub mod broker_message {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MessageType {
        Autocommit = 0,
        Multi = 1,
        Commit = 2,
        Rollback = 3,
        Delete = 4,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriterStatusResponse {
    #[prost(string, repeated, tag = "1")]
    pub knowledgeboxes: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// map of last message processed
    #[prost(map = "string, int64", tag = "2")]
    pub msgid: ::std::collections::HashMap<::prost::alloc::string::String, i64>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriterStatusRequest {}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetLabelsRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub labelset: ::core::option::Option<super::knowledgebox::LabelSet>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DelLabelsRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelsResponse {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag = "2")]
    pub labels: ::core::option::Option<super::knowledgebox::Labels>,
    #[prost(enumeration = "get_labels_response::Status", tag = "3")]
    pub status: i32,
}
/// Nested message and enum types in `GetLabelsResponse`.
pub mod get_labels_response {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelsRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetEntitiesRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub group: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub entities: ::core::option::Option<super::knowledgebox::EntitiesGroup>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesResponse {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(map = "string, message", tag = "2")]
    pub groups: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        super::knowledgebox::EntitiesGroup,
    >,
    #[prost(enumeration = "get_entities_response::Status", tag = "3")]
    pub status: i32,
}
/// Nested message and enum types in `GetEntitiesResponse`.
pub mod get_entities_response {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DelEntitiesRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub group: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MergeEntitiesRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag = "2")]
    pub from: ::core::option::Option<merge_entities_request::EntityId>,
    #[prost(message, optional, tag = "3")]
    pub to: ::core::option::Option<merge_entities_request::EntityId>,
}
/// Nested message and enum types in `MergeEntitiesRequest`.
pub mod merge_entities_request {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntityId {
        #[prost(string, tag = "1")]
        pub group: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub entity: ::prost::alloc::string::String,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelSetRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub labelset: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLabelSetResponse {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag = "2")]
    pub labelset: ::core::option::Option<super::knowledgebox::LabelSet>,
    #[prost(enumeration = "get_label_set_response::Status", tag = "3")]
    pub status: i32,
}
/// Nested message and enum types in `GetLabelSetResponse`.
pub mod get_label_set_response {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesGroupRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub group: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetEntitiesGroupResponse {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag = "2")]
    pub group: ::core::option::Option<super::knowledgebox::EntitiesGroup>,
    #[prost(enumeration = "get_entities_group_response::Status", tag = "3")]
    pub status: i32,
}
/// Nested message and enum types in `GetEntitiesGroupResponse`.
pub mod get_entities_group_response {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetWidgetRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub widget: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetWidgetResponse {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag = "2")]
    pub widget: ::core::option::Option<super::knowledgebox::Widget>,
    #[prost(enumeration = "get_widget_response::Status", tag = "3")]
    pub status: i32,
}
/// Nested message and enum types in `GetWidgetResponse`.
pub mod get_widget_response {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetWidgetsRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetWidgetsResponse {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(map = "string, message", tag = "2")]
    pub widgets:
        ::std::collections::HashMap<::prost::alloc::string::String, super::knowledgebox::Widget>,
    #[prost(enumeration = "get_widgets_response::Status", tag = "3")]
    pub status: i32,
}
/// Nested message and enum types in `GetWidgetsResponse`.
pub mod get_widgets_response {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Notfound = 1,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetWidgetsRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(message, optional, tag = "2")]
    pub widget: ::core::option::Option<super::knowledgebox::Widget>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DetWidgetsRequest {
    #[prost(message, optional, tag = "1")]
    pub kb: ::core::option::Option<super::knowledgebox::KnowledgeBoxId>,
    #[prost(string, tag = "2")]
    pub widget: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OpStatusWriter {
    #[prost(enumeration = "op_status_writer::Status", tag = "1")]
    pub status: i32,
}
/// Nested message and enum types in `OpStatusWriter`.
pub mod op_status_writer {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Ok = 0,
        Error = 1,
        Notfound = 2,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Notification {
    #[prost(int32, tag = "1")]
    pub partition: i32,
    #[prost(string, tag = "2")]
    pub multi: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(int64, tag = "5")]
    pub seqid: i64,
    #[prost(enumeration = "notification::Action", tag = "6")]
    pub action: i32,
}
/// Nested message and enum types in `Notification`.
pub mod notification {
    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Action {
        Commit = 0,
        Abort = 1,
    }
}
//// The member information.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Member {
    //// Member ID.　A string of the UUID.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    //// Cluster listen address. string of IP and port number.
    //// E.g. 127.0.0.1:5000
    #[prost(string, tag = "2")]
    pub listen_address: ::prost::alloc::string::String,
    //// If true, it means self.
    #[prost(bool, tag = "3")]
    pub is_self: bool,
    //// Writer, Node, Reader
    #[prost(string, tag = "4")]
    pub r#type: ::prost::alloc::string::String,
    //// Dummy Member
    #[prost(bool, tag = "5")]
    pub dummy: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListMembersRequest {}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListMembersResponse {
    #[prost(message, repeated, tag = "1")]
    pub members: ::prost::alloc::vec::Vec<Member>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardReplica {
    #[prost(message, optional, tag = "1")]
    pub shard: ::core::option::Option<super::noderesources::ShardCreated>,
    #[prost(string, tag = "2")]
    pub node: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardObject {
    #[prost(string, tag = "1")]
    pub shard: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub replicas: ::prost::alloc::vec::Vec<ShardReplica>,
    #[prost(message, optional, tag = "4")]
    pub timestamp: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Shards {
    #[prost(message, repeated, tag = "1")]
    pub shards: ::prost::alloc::vec::Vec<ShardObject>,
    #[prost(string, tag = "2")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(int32, tag = "3")]
    pub actual: i32,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceFieldId {
    #[prost(string, tag = "1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub rid: ::prost::alloc::string::String,
    #[prost(enumeration = "super::resources::FieldType", tag = "3")]
    pub field_type: i32,
    #[prost(string, tag = "4")]
    pub field: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexResource {
    #[prost(string, tag = "1")]
    pub kbid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub rid: ::prost::alloc::string::String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexStatus {}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceFieldExistsResponse {
    #[prost(bool, tag = "1")]
    pub found: bool,
}
#[doc = r" Generated client implementations."]
pub mod writer_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct WriterClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl WriterClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> WriterClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> WriterClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            WriterClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn get_knowledge_box(
            &mut self,
            request: impl tonic::IntoRequest<super::super::knowledgebox::KnowledgeBoxId>,
        ) -> Result<tonic::Response<super::super::knowledgebox::KnowledgeBox>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetKnowledgeBox");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn new_knowledge_box(
            &mut self,
            request: impl tonic::IntoRequest<super::super::knowledgebox::KnowledgeBoxNew>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::NewKnowledgeBoxResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/NewKnowledgeBox");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_knowledge_box(
            &mut self,
            request: impl tonic::IntoRequest<super::super::knowledgebox::KnowledgeBoxId>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::DeleteKnowledgeBoxResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/DeleteKnowledgeBox");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn update_knowledge_box(
            &mut self,
            request: impl tonic::IntoRequest<super::super::knowledgebox::KnowledgeBoxUpdate>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::UpdateKnowledgeBoxResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/UpdateKnowledgeBox");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn list_knowledge_box(
            &mut self,
            request: impl tonic::IntoRequest<super::super::knowledgebox::KnowledgeBoxPrefix>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::super::knowledgebox::KnowledgeBoxId>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/ListKnowledgeBox");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        pub async fn gc_knowledge_box(
            &mut self,
            request: impl tonic::IntoRequest<super::super::knowledgebox::KnowledgeBoxId>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::GcKnowledgeBoxResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GCKnowledgeBox");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn resource_field_exists(
            &mut self,
            request: impl tonic::IntoRequest<super::ResourceFieldId>,
        ) -> Result<tonic::Response<super::ResourceFieldExistsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/fdbwriter.Writer/ResourceFieldExists");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn process_message(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::BrokerMessage>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/ProcessMessage");
            self.inner
                .client_streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = " Labels"]
        pub async fn get_labels(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLabelsRequest>,
        ) -> Result<tonic::Response<super::GetLabelsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetLabels");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_label_set(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLabelSetRequest>,
        ) -> Result<tonic::Response<super::GetLabelSetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetLabelSet");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_labels(
            &mut self,
            request: impl tonic::IntoRequest<super::SetLabelsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/SetLabels");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn del_labels(
            &mut self,
            request: impl tonic::IntoRequest<super::DelLabelsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/DelLabels");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Entities"]
        pub async fn get_entities(
            &mut self,
            request: impl tonic::IntoRequest<super::GetEntitiesRequest>,
        ) -> Result<tonic::Response<super::GetEntitiesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetEntities");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_entities_group(
            &mut self,
            request: impl tonic::IntoRequest<super::GetEntitiesGroupRequest>,
        ) -> Result<tonic::Response<super::GetEntitiesGroupResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetEntitiesGroup");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_entities(
            &mut self,
            request: impl tonic::IntoRequest<super::SetEntitiesRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/SetEntities");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn del_entities(
            &mut self,
            request: impl tonic::IntoRequest<super::DelEntitiesRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/DelEntities");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Widgets"]
        pub async fn get_widget(
            &mut self,
            request: impl tonic::IntoRequest<super::GetWidgetRequest>,
        ) -> Result<tonic::Response<super::GetWidgetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetWidget");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_widgets(
            &mut self,
            request: impl tonic::IntoRequest<super::GetWidgetsRequest>,
        ) -> Result<tonic::Response<super::GetWidgetsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/GetWidgets");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_widgets(
            &mut self,
            request: impl tonic::IntoRequest<super::SetWidgetsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/SetWidgets");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn del_widgets(
            &mut self,
            request: impl tonic::IntoRequest<super::DetWidgetsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/DelWidgets");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn status(
            &mut self,
            request: impl tonic::IntoRequest<super::WriterStatusRequest>,
        ) -> Result<tonic::Response<super::WriterStatusResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/Status");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn list_members(
            &mut self,
            request: impl tonic::IntoRequest<super::ListMembersRequest>,
        ) -> Result<tonic::Response<super::ListMembersResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/ListMembers");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn index(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexResource>,
        ) -> Result<tonic::Response<super::IndexStatus>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/Index");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn re_index(
            &mut self,
            request: impl tonic::IntoRequest<super::IndexResource>,
        ) -> Result<tonic::Response<super::IndexStatus>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/fdbwriter.Writer/ReIndex");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod writer_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with WriterServer."]
    #[async_trait]
    pub trait Writer: Send + Sync + 'static {
        async fn get_knowledge_box(
            &self,
            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxId>,
        ) -> Result<tonic::Response<super::super::knowledgebox::KnowledgeBox>, tonic::Status>;
        async fn new_knowledge_box(
            &self,
            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxNew>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::NewKnowledgeBoxResponse>,
            tonic::Status,
        >;
        async fn delete_knowledge_box(
            &self,
            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxId>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::DeleteKnowledgeBoxResponse>,
            tonic::Status,
        >;
        async fn update_knowledge_box(
            &self,
            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxUpdate>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::UpdateKnowledgeBoxResponse>,
            tonic::Status,
        >;
        #[doc = "Server streaming response type for the ListKnowledgeBox method."]
        type ListKnowledgeBoxStream: futures_core::Stream<
                Item = Result<super::super::knowledgebox::KnowledgeBoxId, tonic::Status>,
            > + Send
            + 'static;
        async fn list_knowledge_box(
            &self,
            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxPrefix>,
        ) -> Result<tonic::Response<Self::ListKnowledgeBoxStream>, tonic::Status>;
        async fn gc_knowledge_box(
            &self,
            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxId>,
        ) -> Result<
            tonic::Response<super::super::knowledgebox::GcKnowledgeBoxResponse>,
            tonic::Status,
        >;
        async fn resource_field_exists(
            &self,
            request: tonic::Request<super::ResourceFieldId>,
        ) -> Result<tonic::Response<super::ResourceFieldExistsResponse>, tonic::Status>;
        async fn process_message(
            &self,
            request: tonic::Request<tonic::Streaming<super::BrokerMessage>>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        #[doc = " Labels"]
        async fn get_labels(
            &self,
            request: tonic::Request<super::GetLabelsRequest>,
        ) -> Result<tonic::Response<super::GetLabelsResponse>, tonic::Status>;
        async fn get_label_set(
            &self,
            request: tonic::Request<super::GetLabelSetRequest>,
        ) -> Result<tonic::Response<super::GetLabelSetResponse>, tonic::Status>;
        async fn set_labels(
            &self,
            request: tonic::Request<super::SetLabelsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        async fn del_labels(
            &self,
            request: tonic::Request<super::DelLabelsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        #[doc = " Entities"]
        async fn get_entities(
            &self,
            request: tonic::Request<super::GetEntitiesRequest>,
        ) -> Result<tonic::Response<super::GetEntitiesResponse>, tonic::Status>;
        async fn get_entities_group(
            &self,
            request: tonic::Request<super::GetEntitiesGroupRequest>,
        ) -> Result<tonic::Response<super::GetEntitiesGroupResponse>, tonic::Status>;
        async fn set_entities(
            &self,
            request: tonic::Request<super::SetEntitiesRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        async fn del_entities(
            &self,
            request: tonic::Request<super::DelEntitiesRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        #[doc = " Widgets"]
        async fn get_widget(
            &self,
            request: tonic::Request<super::GetWidgetRequest>,
        ) -> Result<tonic::Response<super::GetWidgetResponse>, tonic::Status>;
        async fn get_widgets(
            &self,
            request: tonic::Request<super::GetWidgetsRequest>,
        ) -> Result<tonic::Response<super::GetWidgetsResponse>, tonic::Status>;
        async fn set_widgets(
            &self,
            request: tonic::Request<super::SetWidgetsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        async fn del_widgets(
            &self,
            request: tonic::Request<super::DetWidgetsRequest>,
        ) -> Result<tonic::Response<super::OpStatusWriter>, tonic::Status>;
        async fn status(
            &self,
            request: tonic::Request<super::WriterStatusRequest>,
        ) -> Result<tonic::Response<super::WriterStatusResponse>, tonic::Status>;
        async fn list_members(
            &self,
            request: tonic::Request<super::ListMembersRequest>,
        ) -> Result<tonic::Response<super::ListMembersResponse>, tonic::Status>;
        async fn index(
            &self,
            request: tonic::Request<super::IndexResource>,
        ) -> Result<tonic::Response<super::IndexStatus>, tonic::Status>;
        async fn re_index(
            &self,
            request: tonic::Request<super::IndexResource>,
        ) -> Result<tonic::Response<super::IndexStatus>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct WriterServer<T: Writer> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Writer> WriterServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for WriterServer<T>
    where
        T: Writer,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/fdbwriter.Writer/GetKnowledgeBox" => {
                    #[allow(non_camel_case_types)]
                    struct GetKnowledgeBoxSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer>
                        tonic::server::UnaryService<super::super::knowledgebox::KnowledgeBoxId>
                        for GetKnowledgeBoxSvc<T>
                    {
                        type Response = super::super::knowledgebox::KnowledgeBox;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_knowledge_box(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetKnowledgeBoxSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/NewKnowledgeBox" => {
                    #[allow(non_camel_case_types)]
                    struct NewKnowledgeBoxSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer>
                        tonic::server::UnaryService<super::super::knowledgebox::KnowledgeBoxNew>
                        for NewKnowledgeBoxSvc<T>
                    {
                        type Response = super::super::knowledgebox::NewKnowledgeBoxResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxNew>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).new_knowledge_box(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = NewKnowledgeBoxSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/DeleteKnowledgeBox" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteKnowledgeBoxSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer>
                        tonic::server::UnaryService<super::super::knowledgebox::KnowledgeBoxId>
                        for DeleteKnowledgeBoxSvc<T>
                    {
                        type Response = super::super::knowledgebox::DeleteKnowledgeBoxResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_knowledge_box(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteKnowledgeBoxSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/UpdateKnowledgeBox" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateKnowledgeBoxSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer>
                        tonic::server::UnaryService<super::super::knowledgebox::KnowledgeBoxUpdate>
                        for UpdateKnowledgeBoxSvc<T>
                    {
                        type Response = super::super::knowledgebox::UpdateKnowledgeBoxResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxUpdate>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).update_knowledge_box(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateKnowledgeBoxSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/ListKnowledgeBox" => {
                    #[allow(non_camel_case_types)]
                    struct ListKnowledgeBoxSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer>
                        tonic::server::ServerStreamingService<
                            super::super::knowledgebox::KnowledgeBoxPrefix,
                        > for ListKnowledgeBoxSvc<T>
                    {
                        type Response = super::super::knowledgebox::KnowledgeBoxId;
                        type ResponseStream = T::ListKnowledgeBoxStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxPrefix>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list_knowledge_box(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListKnowledgeBoxSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GCKnowledgeBox" => {
                    #[allow(non_camel_case_types)]
                    struct GCKnowledgeBoxSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer>
                        tonic::server::UnaryService<super::super::knowledgebox::KnowledgeBoxId>
                        for GCKnowledgeBoxSvc<T>
                    {
                        type Response = super::super::knowledgebox::GcKnowledgeBoxResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::knowledgebox::KnowledgeBoxId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).gc_knowledge_box(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GCKnowledgeBoxSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/ResourceFieldExists" => {
                    #[allow(non_camel_case_types)]
                    struct ResourceFieldExistsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::ResourceFieldId> for ResourceFieldExistsSvc<T> {
                        type Response = super::ResourceFieldExistsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ResourceFieldId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).resource_field_exists(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ResourceFieldExistsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/ProcessMessage" => {
                    #[allow(non_camel_case_types)]
                    struct ProcessMessageSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::ClientStreamingService<super::BrokerMessage>
                        for ProcessMessageSvc<T>
                    {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::BrokerMessage>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).process_message(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ProcessMessageSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.client_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GetLabels" => {
                    #[allow(non_camel_case_types)]
                    struct GetLabelsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::GetLabelsRequest> for GetLabelsSvc<T> {
                        type Response = super::GetLabelsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetLabelsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_labels(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetLabelsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GetLabelSet" => {
                    #[allow(non_camel_case_types)]
                    struct GetLabelSetSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::GetLabelSetRequest> for GetLabelSetSvc<T> {
                        type Response = super::GetLabelSetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetLabelSetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_label_set(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetLabelSetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/SetLabels" => {
                    #[allow(non_camel_case_types)]
                    struct SetLabelsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::SetLabelsRequest> for SetLabelsSvc<T> {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetLabelsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_labels(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetLabelsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/DelLabels" => {
                    #[allow(non_camel_case_types)]
                    struct DelLabelsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::DelLabelsRequest> for DelLabelsSvc<T> {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DelLabelsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).del_labels(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DelLabelsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GetEntities" => {
                    #[allow(non_camel_case_types)]
                    struct GetEntitiesSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::GetEntitiesRequest> for GetEntitiesSvc<T> {
                        type Response = super::GetEntitiesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetEntitiesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_entities(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetEntitiesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GetEntitiesGroup" => {
                    #[allow(non_camel_case_types)]
                    struct GetEntitiesGroupSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::GetEntitiesGroupRequest>
                        for GetEntitiesGroupSvc<T>
                    {
                        type Response = super::GetEntitiesGroupResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetEntitiesGroupRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_entities_group(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetEntitiesGroupSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/SetEntities" => {
                    #[allow(non_camel_case_types)]
                    struct SetEntitiesSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::SetEntitiesRequest> for SetEntitiesSvc<T> {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetEntitiesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_entities(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetEntitiesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/DelEntities" => {
                    #[allow(non_camel_case_types)]
                    struct DelEntitiesSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::DelEntitiesRequest> for DelEntitiesSvc<T> {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DelEntitiesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).del_entities(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DelEntitiesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GetWidget" => {
                    #[allow(non_camel_case_types)]
                    struct GetWidgetSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::GetWidgetRequest> for GetWidgetSvc<T> {
                        type Response = super::GetWidgetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetWidgetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_widget(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetWidgetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/GetWidgets" => {
                    #[allow(non_camel_case_types)]
                    struct GetWidgetsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::GetWidgetsRequest> for GetWidgetsSvc<T> {
                        type Response = super::GetWidgetsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetWidgetsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_widgets(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetWidgetsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/SetWidgets" => {
                    #[allow(non_camel_case_types)]
                    struct SetWidgetsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::SetWidgetsRequest> for SetWidgetsSvc<T> {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetWidgetsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_widgets(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetWidgetsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/DelWidgets" => {
                    #[allow(non_camel_case_types)]
                    struct DelWidgetsSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::DetWidgetsRequest> for DelWidgetsSvc<T> {
                        type Response = super::OpStatusWriter;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DetWidgetsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).del_widgets(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DelWidgetsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/Status" => {
                    #[allow(non_camel_case_types)]
                    struct StatusSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::WriterStatusRequest> for StatusSvc<T> {
                        type Response = super::WriterStatusResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WriterStatusRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).status(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = StatusSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/ListMembers" => {
                    #[allow(non_camel_case_types)]
                    struct ListMembersSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::ListMembersRequest> for ListMembersSvc<T> {
                        type Response = super::ListMembersResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListMembersRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list_members(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListMembersSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/Index" => {
                    #[allow(non_camel_case_types)]
                    struct IndexSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::IndexResource> for IndexSvc<T> {
                        type Response = super::IndexStatus;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexResource>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).index(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = IndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/fdbwriter.Writer/ReIndex" => {
                    #[allow(non_camel_case_types)]
                    struct ReIndexSvc<T: Writer>(pub Arc<T>);
                    impl<T: Writer> tonic::server::UnaryService<super::IndexResource> for ReIndexSvc<T> {
                        type Response = super::IndexStatus;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::IndexResource>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).re_index(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Writer> Clone for WriterServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Writer> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Writer> tonic::transport::NamedService for WriterServer<T> {
        const NAME: &'static str = "fdbwriter.Writer";
    }
}
