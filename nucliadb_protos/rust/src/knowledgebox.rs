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
    #[prost(bool, tag="6")]
    pub disable_vectors: bool,
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
    #[prost(bool, tag="3")]
    pub merged: bool,
    #[prost(string, repeated, tag="4")]
    pub represents: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
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
pub struct Widget {
    #[prost(string, tag="1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub description: ::prost::alloc::string::String,
    #[prost(enumeration="widget::WidgetMode", tag="3")]
    pub mode: i32,
    #[prost(message, optional, tag="4")]
    pub features: ::core::option::Option<widget::WidgetFeatures>,
    #[prost(string, repeated, tag="5")]
    pub filters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="6")]
    pub top_entities: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(map="string, string", tag="7")]
    pub style: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
/// Nested message and enum types in `Widget`.
pub mod widget {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WidgetFeatures {
        #[prost(bool, tag="1")]
        pub use_filters: bool,
        #[prost(bool, tag="2")]
        pub suggest_entities: bool,
        #[prost(bool, tag="3")]
        pub suggest_sentences: bool,
        #[prost(bool, tag="4")]
        pub suggest_paragraphs: bool,
        #[prost(bool, tag="5")]
        pub suggest_labels: bool,
        #[prost(bool, tag="6")]
        pub edit_labels: bool,
        #[prost(bool, tag="7")]
        pub entity_annotation: bool,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum WidgetMode {
        Button = 0,
        Input = 1,
        Form = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSet {
    #[prost(int32, tag="1")]
    pub dimension: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorSets {
    #[prost(map="string, message", tag="1")]
    pub vectorsets: ::std::collections::HashMap<::prost::alloc::string::String, VectorSet>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum KnowledgeBoxResponseStatus {
    Ok = 0,
    Conflict = 1,
    Notfound = 2,
    Error = 3,
}
