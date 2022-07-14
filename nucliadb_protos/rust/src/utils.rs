#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Relation {
    #[prost(enumeration="relation::RelationType", tag="5")]
    pub relation: i32,
    #[prost(message, optional, tag="6")]
    pub source: ::core::option::Option<RelationNode>,
    #[prost(message, optional, tag="7")]
    pub to: ::core::option::Option<RelationNode>,
    #[prost(string, tag="8")]
    pub relation_label: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Relation`.
pub mod relation {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum RelationType {
        /// Child resource
        Child = 0,
        /// related with label (GENERATED)
        About = 1,
        /// related with an entity (GENERATED)
        Entity = 2,
        /// related with user (GENERATED)
        Colab = 3,
        /// Synonym relation
        Sym = 4,
        /// related with something
        Other = 5,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RelationNode {
    #[prost(string, tag="4")]
    pub value: ::prost::alloc::string::String,
    #[prost(enumeration="relation_node::NodeType", tag="5")]
    pub ntype: i32,
    #[prost(string, tag="6")]
    pub subtype: ::prost::alloc::string::String,
}
/// Nested message and enum types in `RelationNode`.
pub mod relation_node {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum NodeType {
        Entity = 0,
        Label = 1,
        Resource = 2,
        User = 3,
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
    #[prost(float, repeated, tag="5")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
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
