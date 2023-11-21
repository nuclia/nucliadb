// ID

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxId {
    #[prost(string, tag="1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
}
// GET

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBox {
    #[prost(string, tag="1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(enumeration="KnowledgeBoxResponseStatus", tag="3")]
    pub status: i32,
    #[prost(message, optional, tag="4")]
    pub config: ::core::option::Option<KnowledgeBoxConfig>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxConfig {
    #[prost(string, tag="1")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="3")]
    pub enabled_filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="4")]
    pub enabled_insights: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="5")]
    pub slug: ::prost::alloc::string::String,
    #[deprecated]
    #[prost(bool, tag="6")]
    pub disable_vectors: bool,
    #[prost(int64, tag="7")]
    pub migration_version: i64,
    #[prost(enumeration="super::utils::ReleaseChannel", tag="8")]
    pub release_channel: i32,
}
// NEW

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxNew {
    #[prost(string, tag="1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub config: ::core::option::Option<KnowledgeBoxConfig>,
    #[prost(string, tag="3")]
    pub forceuuid: ::prost::alloc::string::String,
    #[prost(enumeration="super::utils::VectorSimilarity", tag="4")]
    pub similarity: i32,
    #[prost(int32, optional, tag="5")]
    pub vector_dimension: ::core::option::Option<i32>,
    #[prost(float, optional, tag="6")]
    pub default_min_score: ::core::option::Option<f32>,
    #[prost(enumeration="super::utils::ReleaseChannel", tag="7")]
    pub release_channel: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewKnowledgeBoxResponse {
    #[prost(enumeration="KnowledgeBoxResponseStatus", tag="1")]
    pub status: i32,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
}
// SEARCH / LIST

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxPrefix {
    #[prost(string, tag="1")]
    pub prefix: ::prost::alloc::string::String,
}
// UPDATE

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KnowledgeBoxUpdate {
    #[prost(string, tag="1")]
    pub slug: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub config: ::core::option::Option<KnowledgeBoxConfig>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateKnowledgeBoxResponse {
    #[prost(enumeration="KnowledgeBoxResponseStatus", tag="1")]
    pub status: i32,
    #[prost(string, tag="2")]
    pub uuid: ::prost::alloc::string::String,
}
// GC

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GcKnowledgeBoxResponse {
}
// DELETE

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteKnowledgeBoxResponse {
    #[prost(enumeration="KnowledgeBoxResponseStatus", tag="1")]
    pub status: i32,
}
// Clean Index

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CleanedKnowledgeBoxResponse {
}
// Labels on a Knowledge Box

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Label {
    #[prost(string, tag="2")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub related: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub text: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub uri: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelSet {
    #[prost(string, tag="1")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub color: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="3")]
    pub labels: ::prost::alloc::vec::Vec<Label>,
    #[prost(bool, tag="4")]
    pub multiple: bool,
    #[prost(enumeration="label_set::LabelSetKind", repeated, tag="5")]
    pub kind: ::prost::alloc::vec::Vec<i32>,
}
/// Nested message and enum types in `LabelSet`.
pub mod label_set {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum LabelSetKind {
        Resources = 0,
        Paragraphs = 1,
        Sentences = 2,
        Selections = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Labels {
    #[prost(map="string, message", tag="1")]
    pub labelset: ::std::collections::HashMap<::prost::alloc::string::String, LabelSet>,
}
// Entities on a Knowledge Box

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Entity {
    #[prost(string, tag="2")]
    pub value: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="4")]
    pub represents: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag="3")]
    pub merged: bool,
    #[prost(bool, tag="5")]
    pub deleted: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesGroupSummary {
    #[prost(string, tag="2")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub color: ::prost::alloc::string::String,
    #[prost(bool, tag="4")]
    pub custom: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesGroup {
    #[prost(map="string, message", tag="1")]
    pub entities: ::std::collections::HashMap<::prost::alloc::string::String, Entity>,
    #[prost(string, tag="2")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub color: ::prost::alloc::string::String,
    #[prost(bool, tag="4")]
    pub custom: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletedEntitiesGroups {
    #[prost(string, repeated, tag="1")]
    pub entities_groups: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesGroups {
    #[prost(map="string, message", tag="1")]
    pub entities_groups: ::std::collections::HashMap<::prost::alloc::string::String, EntitiesGroup>,
}
///
/// Structure to represent all duplicates defined in a kb
///     - call it an "Index" because it should include flattened version of all duplicated entries
///     - this allows 1 call to pull all duplicates
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityGroupDuplicateIndex {
    #[prost(map="string, message", tag="1")]
    pub entities_groups: ::std::collections::HashMap<::prost::alloc::string::String, entity_group_duplicate_index::EntityGroupDuplicates>,
}
/// Nested message and enum types in `EntityGroupDuplicateIndex`.
pub mod entity_group_duplicate_index {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntityDuplicates {
        #[prost(string, repeated, tag="1")]
        pub duplicates: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EntityGroupDuplicates {
        #[prost(map="string, message", tag="1")]
        pub entities: ::std::collections::HashMap<::prost::alloc::string::String, EntityDuplicates>,
    }
}
// Vectorsets

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSet {
    #[prost(int32, tag="1")]
    pub dimension: i32,
    #[prost(enumeration="super::utils::VectorSimilarity", tag="2")]
    pub similarity: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSets {
    #[prost(map="string, message", tag="1")]
    pub vectorsets: ::std::collections::HashMap<::prost::alloc::string::String, VectorSet>,
}
// Synonyms of a Knowledge Box

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TermSynonyms {
    #[prost(string, repeated, tag="1")]
    pub synonyms: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Synonyms {
    #[prost(map="string, message", tag="1")]
    pub terms: ::std::collections::HashMap<::prost::alloc::string::String, TermSynonyms>,
}
/// Metadata of the model associated to the KB
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SemanticModelMetadata {
    #[prost(enumeration="super::utils::VectorSimilarity", tag="1")]
    pub similarity_function: i32,
    #[prost(int32, optional, tag="2")]
    pub vector_dimension: ::core::option::Option<i32>,
    #[prost(float, optional, tag="3")]
    pub default_min_score: ::core::option::Option<f32>,
}
// Do not update this model without confirmation of internal Learning Config API

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KbConfiguration {
    #[prost(string, tag="2")]
    pub semantic_model: ::prost::alloc::string::String,
    #[prost(string, tag="3")]
    pub generative_model: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub ner_model: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub anonymization_model: ::prost::alloc::string::String,
    #[prost(string, tag="6")]
    pub visual_labeling: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum KnowledgeBoxResponseStatus {
    Ok = 0,
    Conflict = 1,
    Notfound = 2,
    Error = 3,
}
