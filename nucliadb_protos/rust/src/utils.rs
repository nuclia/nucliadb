#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityRelation {
    #[prost(string, tag="1")]
    pub entity: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub entity_type: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Relation {
    #[prost(enumeration="relation::RelationType", tag="1")]
    pub relation: i32,
    #[prost(map="string, string", tag="7")]
    pub properties: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    /// Relations can be with a resource, label, user or entity
    #[prost(oneof="relation::Target", tags="2, 3, 4, 5, 6")]
    pub target: ::core::option::Option<relation::Target>,
    #[prost(oneof="relation::Source", tags="8")]
    pub source: ::core::option::Option<relation::Source>,
}
/// Nested message and enum types in `Relation`.
pub mod relation {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum RelationType {
        /// Child resource
        Child = 0,
        /// related with label (GENERATED)
        About = 2,
        /// related with an entity (GENERATED)
        Entity = 3,
        /// related with user (GENERATED)
        Colab = 4,
        /// related with something
        Other = 5,
    }
    /// Relations can be with a resource, label, user or entity
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Target {
        #[prost(string, tag="2")]
        Resource(::prost::alloc::string::String),
        #[prost(string, tag="3")]
        Label(::prost::alloc::string::String),
        #[prost(string, tag="4")]
        User(::prost::alloc::string::String),
        #[prost(message, tag="5")]
        Entity(super::EntityRelation),
        #[prost(string, tag="6")]
        Other(::prost::alloc::string::String),
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Source {
        #[prost(message, tag="8")]
        FromEntity(super::EntityRelation),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractedText {
    #[prost(string, tag="1")]
    pub text: ::prost::alloc::string::String,
    #[prost(map="string, string", tag="2")]
    pub split_text: ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub deleted_splits: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vector {
    #[prost(int32, tag="1")]
    pub start: i32,
    #[prost(int32, tag="2")]
    pub end: i32,
    #[prost(int32, tag="3")]
    pub start_paragraph: i32,
    #[prost(int32, tag="4")]
    pub end_paragraph: i32,
    #[prost(bytes="vec", tag="5")]
    pub vector: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vectors {
    #[prost(message, repeated, tag="1")]
    pub vectors: ::prost::alloc::vec::Vec<Vector>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorObject {
    #[prost(message, optional, tag="1")]
    pub vectors: ::core::option::Option<Vectors>,
    #[prost(map="string, message", tag="2")]
    pub split_vectors: ::std::collections::HashMap<::prost::alloc::string::String, Vectors>,
    #[prost(string, repeated, tag="3")]
    pub deleted_splits: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
