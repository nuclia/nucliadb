// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxId {
    #[prost(string, tag = "1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub uuid: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreatePineconeConfig {
    #[prost(string, tag = "1")]
    pub api_key: ::prost::alloc::string::String,
    #[prost(enumeration = "PineconeServerlessCloud", tag = "2")]
    pub serverless_cloud: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PineconeIndexMetadata {
    #[prost(string, tag = "1")]
    pub index_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub index_host: ::prost::alloc::string::String,
    #[prost(int32, tag = "3")]
    pub vector_dimension: i32,
    #[prost(enumeration = "super::utils::VectorSimilarity", tag = "4")]
    pub similarity: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoredPineconeConfig {
    #[prost(string, tag = "1")]
    pub encrypted_api_key: ::prost::alloc::string::String,
    /// vectorset id -> PineconeIndexMetadata
    #[prost(map = "string, message", tag = "2")]
    pub indexes: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        PineconeIndexMetadata,
    >,
    #[prost(enumeration = "PineconeServerlessCloud", tag = "3")]
    pub serverless_cloud: i32,
}
/// External Index node provider
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExternalIndexProviderMetadata {
    #[prost(enumeration = "ExternalIndexProviderType", tag = "1")]
    pub r#type: i32,
    #[prost(oneof = "create_external_index_provider_metadata::Config", tags = "2")]
    pub config: ::core::option::Option<create_external_index_provider_metadata::Config>,
}
/// Nested message and enum types in `CreateExternalIndexProviderMetadata`.
pub mod create_external_index_provider_metadata {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Config {
        #[prost(message, tag = "2")]
        PineconeConfig(super::CreatePineconeConfig),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StoredExternalIndexProviderMetadata {
    #[prost(enumeration = "ExternalIndexProviderType", tag = "1")]
    pub r#type: i32,
    #[prost(oneof = "stored_external_index_provider_metadata::Config", tags = "2")]
    pub config: ::core::option::Option<stored_external_index_provider_metadata::Config>,
}
/// Nested message and enum types in `StoredExternalIndexProviderMetadata`.
pub mod stored_external_index_provider_metadata {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Config {
        #[prost(message, tag = "2")]
        PineconeConfig(super::StoredPineconeConfig),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxConfig {
    #[prost(string, tag = "1")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub slug: ::prost::alloc::string::String,
    #[prost(int64, tag = "7")]
    pub migration_version: i64,
    #[prost(message, optional, tag = "9")]
    pub external_index_provider: ::core::option::Option<
        StoredExternalIndexProviderMetadata,
    >,
    #[deprecated]
    #[prost(string, repeated, tag = "3")]
    pub enabled_filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[deprecated]
    #[prost(string, repeated, tag = "4")]
    pub enabled_insights: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[deprecated]
    #[prost(bool, tag = "6")]
    pub disable_vectors: bool,
    /// DEPRECATED: duplicated field also stored in `writer.proto Shards`
    #[deprecated]
    #[prost(enumeration = "super::utils::ReleaseChannel", tag = "8")]
    pub release_channel: i32,
    #[prost(bool, tag = "10")]
    pub hidden_resources_enabled: bool,
    #[prost(bool, tag = "11")]
    pub hidden_resources_hide_on_creation: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxUpdate {
    #[prost(string, tag = "1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub config: ::core::option::Option<KnowledgeBoxConfig>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateKnowledgeBoxResponse {
    #[prost(enumeration = "KnowledgeBoxResponseStatus", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub uuid: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteKnowledgeBoxResponse {
    #[prost(enumeration = "KnowledgeBoxResponseStatus", tag = "1")]
    pub status: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag = "2")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub related: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub text: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub uri: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelSet {
    #[prost(string, tag = "1")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub color: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub labels: ::prost::alloc::vec::Vec<Label>,
    #[prost(bool, tag = "4")]
    pub multiple: bool,
    #[prost(enumeration = "label_set::LabelSetKind", repeated, tag = "5")]
    pub kind: ::prost::alloc::vec::Vec<i32>,
}
/// Nested message and enum types in `LabelSet`.
pub mod label_set {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum LabelSetKind {
        Resources = 0,
        Paragraphs = 1,
        Sentences = 2,
        Selections = 3,
    }
    impl LabelSetKind {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                LabelSetKind::Resources => "RESOURCES",
                LabelSetKind::Paragraphs => "PARAGRAPHS",
                LabelSetKind::Sentences => "SENTENCES",
                LabelSetKind::Selections => "SELECTIONS",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "RESOURCES" => Some(Self::Resources),
                "PARAGRAPHS" => Some(Self::Paragraphs),
                "SENTENCES" => Some(Self::Sentences),
                "SELECTIONS" => Some(Self::Selections),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Labels {
    #[prost(map = "string, message", tag = "1")]
    pub labelset: ::std::collections::HashMap<::prost::alloc::string::String, LabelSet>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entity {
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "4")]
    pub represents: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "3")]
    pub merged: bool,
    #[prost(bool, tag = "5")]
    pub deleted: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesGroupSummary {
    #[prost(string, tag = "2")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub color: ::prost::alloc::string::String,
    #[prost(bool, tag = "4")]
    pub custom: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesGroup {
    #[prost(map = "string, message", tag = "1")]
    pub entities: ::std::collections::HashMap<::prost::alloc::string::String, Entity>,
    #[prost(string, tag = "2")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub color: ::prost::alloc::string::String,
    #[prost(bool, tag = "4")]
    pub custom: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletedEntitiesGroups {
    #[prost(string, repeated, tag = "1")]
    pub entities_groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesGroups {
    #[prost(map = "string, message", tag = "1")]
    pub entities_groups: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        EntitiesGroup,
    >,
}
///
/// Structure to represent all duplicates defined in a kb
///      - call it an "Index" because it should include flattened version of all duplicated entries
///      - this allows 1 call to pull all duplicates
///
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityGroupDuplicateIndex {
    #[prost(map = "string, message", tag = "1")]
    pub entities_groups: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        entity_group_duplicate_index::EntityGroupDuplicates,
    >,
}
/// Nested message and enum types in `EntityGroupDuplicateIndex`.
pub mod entity_group_duplicate_index {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntityDuplicates {
        #[prost(string, repeated, tag = "1")]
        pub duplicates: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntityGroupDuplicates {
        #[prost(map = "string, message", tag = "1")]
        pub entities: ::std::collections::HashMap<
            ::prost::alloc::string::String,
            EntityDuplicates,
        >,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSet {
    #[prost(int32, tag = "1")]
    pub dimension: i32,
    #[prost(enumeration = "super::utils::VectorSimilarity", tag = "2")]
    pub similarity: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSets {
    #[prost(map = "string, message", tag = "1")]
    pub vectorsets: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        VectorSet,
    >,
}
/// Configuration values for a vectorset
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSetConfig {
    #[prost(string, tag = "1")]
    pub vectorset_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub vectorset_index_config: ::core::option::Option<
        super::nodewriter::VectorIndexConfig,
    >,
    /// list of possible subdivisions of the matryoshka embeddings (if the model
    /// supports it)
    #[prost(uint32, repeated, tag = "3")]
    pub matryoshka_dimensions: ::prost::alloc::vec::Vec<u32>,
}
/// KB vectorsets and their configuration
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxVectorSetsConfig {
    #[prost(message, repeated, tag = "1")]
    pub vectorsets: ::prost::alloc::vec::Vec<VectorSetConfig>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TermSynonyms {
    #[prost(string, repeated, tag = "1")]
    pub synonyms: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Synonyms {
    #[prost(map = "string, message", tag = "1")]
    pub terms: ::std::collections::HashMap<::prost::alloc::string::String, TermSynonyms>,
}
/// Metadata of the model associated to the KB
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SemanticModelMetadata {
    #[prost(enumeration = "super::utils::VectorSimilarity", tag = "1")]
    pub similarity_function: i32,
    #[prost(int32, optional, tag = "2")]
    pub vector_dimension: ::core::option::Option<i32>,
    #[deprecated]
    #[prost(float, optional, tag = "3")]
    pub default_min_score: ::core::option::Option<f32>,
    /// list of possible subdivisions of the matryoshka embeddings (if the model
    /// supports it)
    #[prost(uint32, repeated, tag = "4")]
    pub matryoshka_dimensions: ::prost::alloc::vec::Vec<u32>,
}
/// Deprecated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KbConfiguration {
    #[prost(string, tag = "2")]
    pub semantic_model: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub generative_model: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub ner_model: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub anonymization_model: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub visual_labeling: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum KnowledgeBoxResponseStatus {
    Ok = 0,
    Conflict = 1,
    Notfound = 2,
    Error = 3,
    ExternalIndexProviderError = 4,
}
impl KnowledgeBoxResponseStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            KnowledgeBoxResponseStatus::Ok => "OK",
            KnowledgeBoxResponseStatus::Conflict => "CONFLICT",
            KnowledgeBoxResponseStatus::Notfound => "NOTFOUND",
            KnowledgeBoxResponseStatus::Error => "ERROR",
            KnowledgeBoxResponseStatus::ExternalIndexProviderError => {
                "EXTERNAL_INDEX_PROVIDER_ERROR"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "OK" => Some(Self::Ok),
            "CONFLICT" => Some(Self::Conflict),
            "NOTFOUND" => Some(Self::Notfound),
            "ERROR" => Some(Self::Error),
            "EXTERNAL_INDEX_PROVIDER_ERROR" => Some(Self::ExternalIndexProviderError),
            _ => None,
        }
    }
}
/// External Index node provider
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ExternalIndexProviderType {
    Unset = 0,
    Pinecone = 1,
}
impl ExternalIndexProviderType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ExternalIndexProviderType::Unset => "UNSET",
            ExternalIndexProviderType::Pinecone => "PINECONE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNSET" => Some(Self::Unset),
            "PINECONE" => Some(Self::Pinecone),
            _ => None,
        }
    }
}
/// Pinecone
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PineconeServerlessCloud {
    PineconeUnset = 0,
    AwsUsEast1 = 1,
    AwsUsWest2 = 2,
    AwsEuWest1 = 3,
    GcpUsCentral1 = 4,
    AzureEastus2 = 5,
}
impl PineconeServerlessCloud {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PineconeServerlessCloud::PineconeUnset => "PINECONE_UNSET",
            PineconeServerlessCloud::AwsUsEast1 => "AWS_US_EAST_1",
            PineconeServerlessCloud::AwsUsWest2 => "AWS_US_WEST_2",
            PineconeServerlessCloud::AwsEuWest1 => "AWS_EU_WEST_1",
            PineconeServerlessCloud::GcpUsCentral1 => "GCP_US_CENTRAL1",
            PineconeServerlessCloud::AzureEastus2 => "AZURE_EASTUS2",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "PINECONE_UNSET" => Some(Self::PineconeUnset),
            "AWS_US_EAST_1" => Some(Self::AwsUsEast1),
            "AWS_US_WEST_2" => Some(Self::AwsUsWest2),
            "AWS_EU_WEST_1" => Some(Self::AwsEuWest1),
            "GCP_US_CENTRAL1" => Some(Self::GcpUsCentral1),
            "AZURE_EASTUS2" => Some(Self::AzureEastus2),
            _ => None,
        }
    }
}
